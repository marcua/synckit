_skProto = function() {

    // Error Check -- Make sure 'new' keyword was used
    // ----------------------------------------------------------------
    if ( !(this instanceof _skProto) ) 
       return new _skProto();

    var _me = this;

    // Member Variables
    // ----------------------------------------------------------------
    _me._localdb = null;
    _me._timers = null;

    _me._bulkloadTime = 0;
    _me._dataTransferTime = 0;
    _me._templateTime = 0;
    _me._queryParams = {};
    
    // Methods
    // ----------------------------------------------------------------
    _me.opendb = function(name) {
        if (_me._localdb !== null) {
            _me._localdb.open(name);
        }
        else {
            console.error("Local DB is null.");
        }
    };

    _me.timeStart = function(obj) {
        _me._timers[obj] = (new Date).getTime();
    };
    
    _me.timeEnd = function(obj) {
        if (_me._timers[obj]) {
            var diff = (new Date).getTime() - _me._timers[obj];
            _me._timers[obj] = null;            
            return diff;
        }
        return null;
    };
    
    _me.table_exists = function(table) {
        var result = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", [table]);
        var retval = result.isValidRow();
        result.close();
        return retval;
    };
    
    _me.create_tables = function() {
//        console.info("Creating Stats Table");
        if (_me._localdb !== null) {
            _me.execute("CREATE TABLE IF NOT EXISTS sk_endpoints (id integer PRIMARY KEY AUTOINCREMENT, endpoint_uri varchar(512), UNIQUE (endpoint_uri));").close();
            _me.execute("CREATE TABLE IF NOT EXISTS sk_views (endpoint_id integer REFERENCES sk_endpoints(id), name varchar(128), schema text, syncspec text, vshash varchar(32), UNIQUE (endpoint_id, name));").close();
        } else {
            console.error("Could not create stats table. Local DB is null.");
        }        
    };
    
    _me.reset = function() {
//        console.info("Resetting DB");
        if (_me._localdb !== null) {
            _me._localdb.remove();
            _me.opendb("synckit");
            _me.create_tables();
        }
        else {
            console.error("Local DB is null.");
        }
    };

    /* When _me function completes, the given endpoint will update the
     * sk_endpoints and sk_views tables to contain the most up-to-date
     * information on the view with name viewname and viewspec.  If the old
     * view is out of date (the view's stored vshash is not the one in the
     * viewspec), the table will be updated with the new viewspec
     * information, the old view will be dropped, and a new view will be
     * created in its stead. */
    _me.build_view = function(endpoint, viewname, viewspec) {
        var endpoint_id = _me.get_or_create_endpoint_id(endpoint);
        var view_res = _me.execute("SELECT vshash FROM sk_views WHERE endpoint_id = ? and name = ?;", [endpoint_id, viewname]);
        if (!view_res.isValidRow()) {
            view_res.close();
            // if it doesn't exist, create it.
            // create an entry in sk_views
            _me.execute("INSERT INTO sk_views (endpoint_id, name, schema, syncspec, vshash) VALUES (?, ?, ?, ?, ?);", [endpoint_id, viewname, JSON.stringify(viewspec.schema), JSON.stringify(viewspec.syncspec), viewspec.vshash]).close();
            // create a table for the view
            _me.create_view_table(_me.view_table_name(endpoint_id, viewname), viewspec.schema, viewspec.syncspec)
        } else if (viewspec.vshash != view_res.fieldByName('vshash')) {
            view_res.close();
            // if it exists, but had an outdated id, re-create it
            // update the entry in sk_views
            _me.execute("UPDATE sk_views SET schema = ?, syncspec = ?, vshash = ? WHERE endpoint_id = ? AND name = ?;", [JSON.stringify(viewspec.schema), JSON.stringify(viewspec.syncspec), viewspec.vshash, endpoint_id, viewname]).close();
            var view_table_name = _me.view_table_name(endpoint_id, viewname);
            // drop the old view's table
            _me.execute("DROP TABLE IF EXISTS " + view_table_name + ";").close();
            // create the new view's table
            _me.create_view_table(view_table_name, viewspec.schema, viewspec.syncspec)
        }
    };

    /* Returns the ID of the endpoint uri, creating an entry if one doesn't
     * exist.*/
    _me.get_or_create_endpoint_id = function(endpoint) {
        endpoint_res = _me.execute("SELECT id FROM sk_endpoints WHERE endpoint_uri = ?;", [endpoint]);
        endpoint_id = 0;
        if (!endpoint_res.isValidRow()) {
            endpoint_res.close();
            _me.execute("INSERT INTO sk_endpoints (endpoint_uri) VALUES (?);", [endpoint]).close();
            endpoint_res = _me.execute("SELECT id FROM sk_endpoints WHERE endpoint_uri = ?;", [endpoint]);
        }
        var retval = endpoint_res.fieldByName('id');
        endpoint_res.close();
        return retval;
    };

    /* Returns the name of the view (table) in the database for the view
     * named viewname in the endpoint specified */
    _me.view_table_name = function(endpoint_id, viewname) {
        return "sk_" + viewname + endpoint_id;
    };
   
    /* Returns view information for the view at endpoint with
     * id=endpoint_id, and name specified by viewname.  fields is an array
     * of fields to be returned in a dictionary.  Possibilities are:
     * "syncspec" "vshash and "schema". 
     * NOTE: SQL Injection Hack possible if 'fields' contains illegal field
     * names.  Make sure to only call with safe field names.*/
    _me.get_view_info = function(endpoint_id, viewname, fields) {
        var sql = "SELECT ";
        for (var fieldnum in fields) {
            sql += fields[fieldnum] + ",";
        }
        sql = sql.substr(0, sql.length - 1);
        sql += " FROM sk_views WHERE endpoint_id = ? AND name = ?;";
        var res = _me.execute(sql, [endpoint_id, viewname]);
        var ret = {}
        if (res.isValidRow()) {
            for (var fieldnum = 0; fieldnum < fields.length; fieldnum++) {
                var field_data = res.field(fieldnum);
                var field = fields[fieldnum];
                if (field == "syncspec" || field == "schema") {
                    field_data = JSON.parse(field_data);
                }
                ret[field] = field_data;
            }
        } else {
            console.log('Could not find view in get_view_info: ' + viewname);
        }
        res.close();
        return ret;
    };
    
    /* Build the view table and any necessary indices on the table
     * depending on the type of syncspec specified. 
     * NOTE/potential sql injection attack: because we can't use prepared 
     * statements for CREATE TABLE statements, be careful what table/field 
     * names you pass into this function.*/
    _me.create_view_table = function(view_table_name, schema, syncspec) {
        var sql = "CREATE TABLE " + view_table_name + " ("; 
        for (var i = 0; i < schema.length; i++) {
            sql += schema[i] + ",";
        }
        sql = sql.substr(0, sql.length - 1);
        if (syncspec.__type === "queue") {
            sql += ", UNIQUE (" + syncspec.sortfield + ")";
        } else if (syncspec.__type === "set") {
            sql += ", UNIQUE (" + syncspec.idfield + ")";
        }
        // TODO: do we need to index cube fields?
        sql += ");";
        _me.execute(sql).close();
    };
    
    /* Synchronizes the local database with endpoint_uri.
     *   views_to_sync is a list of the view names of the views you wish to sync.
     *   extra_view_params is a dictionary of the form
     *       { "viewname1": {param1: val1, param2: val2},
     *         "viewname2": {param3: val3} }
     *     these parameters will be added to the query for each view.
     *     they are useful for things like sets, which must append 
     *     the ID of the item being queried (e.g., {"setname": {"filter": 30}})
     *   extra_query_params is a flat dictionary which contains arguments
     *     that will be appended to the query dictionary and apply
     *     query-wide.  arguments such as "__latency" and "__bandwidth"
     *     should be added to this dictionary. */
    _me.sync = function(endpoint_uri, views_to_sync, extra_view_params, extra_query_params, callback) {
        var endpoint_id = _me.get_or_create_endpoint_id(endpoint_uri);
        var query = _me.generate_query(endpoint_id, views_to_sync, extra_view_params);
        _me.issue_query(endpoint_uri, endpoint_id, query, extra_query_params, callback);
    };

    /* Returns the state of each of the views for a given endpoint.  _me
     * can be sent to the server to receive an updated state.  endpoint_id
     * represents the URI of the endpoint being queried. views_to_sync and
     * extra_view_params are documented in _me.sync. */
    _me.generate_query = function(endpoint_id, views_to_sync, extra_view_params) {
        if (_me._localdb !== null) {
            var query = {};
            // loop through the views we wish to sync.
            for (var viewnum in views_to_sync) {
                var viewname = views_to_sync[viewnum];
                var view_info = _me.get_view_info(endpoint_id, viewname, ['syncspec', 'vshash']);
                var syncspec = view_info.syncspec
                var vshash = view_info.vshash
                if (syncspec.__type == "set") {
                    _me.set_query(endpoint_id, viewname, syncspec, extra_view_params[viewname], query);
                }
                else if (syncspec.__type == "queue") {
                    _me.queue_query(endpoint_id, viewname, syncspec, extra_view_params[viewname], query);
                }
                else if (syncspec.__type == "cube") {
                    _me.cube_query(endpoint_id, viewname, syncspec, extra_view_params[viewname], query);
                }

                if (viewname in query) {
                    query[viewname].__vshash = vshash;
                }
            }
            return query;
        }
        else {
            console.error("Local DB is null. No Stats available");
            return {};
        }        
    };

    /* Configures the 'query' dictionary to append a query for the endpoint
     * and view specified by viewname, whose type is a set.  The syncspec
     * contains sync information for this set, and extra_params may be
     * empty, or contain a 'filter' argument, which specifies which id to
     * ask for specifically.  'query' will only be updated if there is no
     * 'filter' parameter, or if the id specified by the 'filter' parameter
     * does not exist in the current view. */
    _me.set_query = function(endpoint_id, viewname, syncspec, extra_params, query) {
        if (syncspec.__type != "set") {
            console.log("Configuring a set that isn't: " + view_name);
            return;
        }
        var send_query = true;
        var sq = {};
        var table = _me.view_table_name(endpoint_id, viewname);

        // Do we want to filter only a specific row?
        if (extra_params && ('filter' in extra_params)) {
            var theid = extra_params.filter;
            var sql = "SELECT " + syncspec.idfield + " FROM " + table;
            sql += " WHERE " + syncspec.idfield + "=?;";
            var res = _me.execute(sql, [theid]);
            // If the row doesn't already exist, we continue with the query.
            // Otherwise, we've got the row, and don't send the query.
            if (! res.isValidRow()) {
                sq.filter = [theid];
            } else {
                send_query = false;
            }
            res.close();
        }

        // send_query is true if there was no filter, or if the filtered id
        // wasn't found.
        if (send_query) {
            // generate a list of ids we already have and add those to the
            // query.
            var sql = "SELECT " + syncspec.idfield + " FROM " + table + ";";
            var res = _me.execute(sql);
            var already = [];
            while (res.isValidRow()) {
                already.push(res.field(0));
                res.next();
            }
            if (already.length > 0) {
                sq.exclude = already;	
            }
            res.close();
            query[viewname] = sq;
        }
    };

    /* Configures the 'query' dictionary to append a query for the endpoint
     * and view specified by viewname, whose type is a queue.  The syncspec
     * contains sync information for this queue, and extra_params may be
     * empty, or contain a 'now' argument, which is only used for
     * testing.  'now' can be set to the date at which to display the
     * queue, to facilitate time-travel: no row with sortfield greater than
     * the 'now' value will be displayed. */
    _me.queue_query = function(endpoint_id, viewname, syncspec, extra_params, query) {
        if (syncspec.__type != "queue") {
            console.log("Configuring a queue that isn't: " + view_name);
            return;
        }

        var sq = {};
        var minmax;
        if (syncspec.order == "DESC") {
            minmax = "min";
        }
        else if (syncspec.order == "ASC") {
            minmax = "max";
        }
        var table = _me.view_table_name(endpoint_id, viewname);
        var stmt = "SELECT " + minmax;
        stmt += "(" + syncspec.sortfield + ") FROM " + table + ";";
        var res  = _me.execute(stmt);
        if (res.isValidRow() && (res.field(0) != null)) {
            sq[minmax] = res.field(0);
        }
        res.close();
        if (extra_params && ('now' in extra_params)) {
            sq.now = extra_params.now;
        }
        query[viewname] = sq;
    };

    /* Configures the 'query' dictionary to append a query for the endpoint
     * and view specified by viewname, whose type is a data cube.  The syncspec
     * contains sync information for this cube.  Currently, there is no
     * special functionality of the state of the cube, so an empty query is
     * always sent for the view. */
    _me.cube_query = function(endpoint_id, viewname, syncspec, extra_params, query) {
        if (syncspec.__type != "cube") {
            console.log("Configuring a cube that isn't: " + view_name);
            return;
        }
        query[viewname] = {};
    };
    
    /* Sends the query to the endpoint after appending extra_query_params
     * to it.  Once a response comes back, _me.bulkload will be called
     * asynchronously, which will call the callback that the user specified
     * after properly syncing the local database. */
    _me.issue_query = function(endpoint_uri, endpoint_id, query, extra_query_params, callback) {
        var numViews = 0;
        for (view in query) {
            numViews++;
        }
        if (numViews > 0) {
            var params = {"queries":JSON.stringify(query)};
            for (var key in extra_query_params) {
                params[key] = extra_query_params[key];
            }
            
            _me._queryParams = params;            
            _me.timeStart("xfer");
            jQuery.post(endpoint_uri, params, function(response) {
                _me._dataTransferTime = _me.timeEnd("xfer");
                _me.bulkload(response, endpoint_id, endpoint_uri, callback);
            }, "json");
        } else {
            callback();
        }

    };

    _me.execute = function(statement, args) {
        if (_me._localdb !== null) {
//            console.log("$$$" + statement);
//            console.log(args);
            return _me._localdb.execute(statement, args);                
        }
        else {
            console.error("Can't execute query. Local DB is null.");
        }
    };

    // Returns the result of a query as a JSON statement
    _me.json_results_for = function(statement, args) {
        if (_me._localdb !== null) {
            var res = _me._localdb.execute(statement, args);
            var ans = [];
            while (res.isValidRow()) {
                var obj = {};
                for (var i=0; i<res.fieldCount(); i++) {
                    obj[res.fieldName(i)] = res.field(i);
                }
                ans[ans.length] = obj;
                res.next();
            }
            res.close();
            return ans;      
        }
        else {
            console.error("Can't execute query. Local DB is null.");
        }
    };

    // Takes a hash table where keys are the label
    // and values are the sql statements
    _me.process_data_spec = function(data_spec) {
        var ret = {};
        for (var key in data_spec) {
            var val = data_spec[key];
            if (typeof(val) == "string") {
                ret[key] = _me.json_results_for(val);
            }
            else if (typeof(val) == "object") {
                ret[key] = _me.process_data_spec(val);
            }
        }
        return ret;
    };
    
    _me.bulkload = function(response, endpoint_id, endpoint_uri, callback) {
        _me.timeStart("bulkload");
        for (var viewname in response) {
            var viewdata = response[viewname];
            if (typeof(viewdata) == "string") {
                console.info("Skipping view " + viewname + " with message: " + viewdata);
                continue;
            }
            
            // if a viewspec comes back, the server is signalling that we
            // might have a new schema for this view.  try to rebuild the view.
            if ("viewspec" in viewdata) {
                _me.build_view(endpoint_uri, viewname, viewdata.viewspec);
            }

            // Insert the results into the view if any exist
            var results = viewdata.results;
            if (results.length == 0) {
            	continue;
            }
            var viewtable = _me.view_table_name(endpoint_id, viewname);
            var sqlStatement = "INSERT INTO " + viewtable + " VALUES (";
            for (var z = 0; z<results[0].length; z++) {
                sqlStatement += "?,";
            }   
            sqlStatement = sqlStatement.substr(0,sqlStatement.length - 1);
            sqlStatement += ");";
            
            //_me.execute("BEGIN;");
            for (var rownum in results) {
                _me.execute(sqlStatement, results[rownum]);
            }
            //_me.execute("COMMIT;");
        }
        // Alert the sync requester that the job is done.
        _me._bulkloadTime = _me.timeEnd("bulkload");
        callback();
    };

    
    // Debugging Methods
    // ----------------------------------------------------------------
    // 
    _me.dump_stats = function(table) {
        if (typeof(table) == "undefined") {
            // Get a list of all tables
            var result = _me.execute("SELECT name FROM sqlite_master WHERE type='table';");
            while (result.isValidRow()) {
                console.info("===" + result.field(0) + "===");
                _me.dump_stats(result.field(0));
                console.info("=============================");
                console.info("=============================");
                result.next();
            }
            var retval = result.isValidRow();
            result.close();
            return retval;
        }
    };
    
    _me.dump = function(table) {
        if (typeof(table) == "undefined") {
            // Get a list of all tables
            var result = _me.execute("SELECT name FROM sqlite_master WHERE type='table';");
            while (result.isValidRow()) {
                console.info("===" + result.field(0) + "===");
                _me.dump(result.field(0));
                console.info("=============================");
                console.info("=============================");
                result.next();
            }
            var retval = result.isValidRow();
            result.close();
            return retval;
            
        }
        
        if (_me._localdb !== null) {
            var rs = _me.execute("SELECT * FROM " + table + ";");
            while (rs.isValidRow()) {
              for (var i=0; i<rs.fieldCount(); i++) {
                  console.log(rs.fieldName(i) + ": " + rs.field(i));
              }
              console.log("----");
              rs.next();
            }
            rs.close();
        }
        else {
            console.error("Can't dump. Local DB is null.");
        }
    };
    
    // Initialization
    // ----------------------------------------------------------------

    if ((typeof(google) != "undefined") &&
        (typeof(google.gears) != "undefined")) {
        
        _me._localdb = google.gears.factory.create('beta.database');
        _me._timers = {};
        _me.opendb("synckit"); 
        _me.create_tables();
    }
    else {
        _me._localdb = null;
        console.error("Google Gears not found.");
    }
    
}; // end _skProto = function() {

create_synckit = function() {
    var db = new _skProto();
    return db;
};

function urlParam(name){
	var results = new RegExp('[#&]' + name + '=([^&#]*)').exec(window.location.href);
	if (results) {
		return results[1].replace(/%20/,' ')
	}
	return 0;
}

