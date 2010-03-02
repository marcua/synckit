
jQuery.fn.template = function(value) {
	if (value === undefined) {
    	return this.data("backstage_template");        
    }
    else {
        this.data("backstage_template", value);
        return this;
    }
};

jQuery.fn.templateData = function(value) {
	if (value === undefined) {
    	return this.data("backstage_templateData");        
    }
    else {
        this.data("backstage_templateData", value);
        return this;
    }
};

jQuery.fn.render_new = function(callback) {
	window.synckit.timeStart("Rendering SQL Template");
	var query = this.attr('query');
	var templates = this.find("[itemscope]");
	var res = window.synckit.execute(query);
	var map = {};
	
	while (res.isValidRow()) {
		// Loop over each item to do
		templates.each(function(i) {
			jQuery(this).find("[itemprop]").each(function(j) {
				var prop = jQuery(this).attr("itemprop");
				var val = res.fieldByName(prop);
				jQuery(this).html(val);
			});
			jQuery(this).before(jQuery(this).clone());
		});
		res.next();
    }

	templates.each(function(i) {
		jQuery(this).remove();
	});

	window.synckit.timeEnd("Rendering SQL Template");
	if (callback !== undefined) {
		callback.call();
	}
}

jQuery.fn.render_flying = function(results) { 
    
	var templates = this.find("[itemscope]");
	var map = {};
	
	for (var i=0; i<results.length; i++) {
		// Loop over each item to do
		templates.each(function(i) {
			jQuery(this).find("[itemprop]").each(function(j) {
				var prop = jQuery(this).attr("itemprop");
				var val = results[i][replacementFieldByName(prop)];
				jQuery(this).html(val);
			});
			jQuery(this).before(jQuery(this).clone());
		});
    }

	templates.each(function(i) {
		jQuery(this).remove();
	});
}




