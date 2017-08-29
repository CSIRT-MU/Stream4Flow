var texts = [];
var tooltipTexts = [];
var scaleXLabels = [];
// Generate the Top N chart
function generateTopN(type, dataCsv, number) {
    // Elements ID
    var chartId = 'chart-dns-stats-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    var chart_height = 450;//$('#' + chartId).height();
    var font_angle = '0';
    var font_offset = '0px';
    var fontColor = "white";
    var placement = "bottom-in";
    var tooltip_text = "%t\n%v";
    var value_text = "%t";
    var chartXOffset = '0px';
    var max_value = 0;
    var top_values = parseInt(number);

    // Prepare title
    var chart_title = ""
    if (type == "record_type") {
        if (top_values > 10) {
            font_offset = '20px';
            font_angle = '-90';
        }
        chart_title = "Top DNS Record Types";
        placement = "top-in";
    } else if (type == "response_code") {
        chart_title = "Top DNS Response Codes";
        font_angle = '0';
        placement = "top-in";
    } else if (type == "queried_domain") {
        chart_title = "Top Queried Domains";
        chart_height += (top_values > 10) ? (top_values * 7) : 0;
    } else if (type == "queried_local") {
        chart_title = "Top Queried Local DNS Servers From Outside Network";
        tooltip_text = "%t\n%v packets";
        chart_height += (top_values > 10) ? (top_values * 7) : 0;
    } else if (type == "external_dns") {
        chart_title = "Top Queried External DNS Servers";
        chart_height += (top_values > 10) ? (top_values * 7) : 0;
    } else if (type == "queried_by_ip") {
        chart_title = "Device with the most records for domain";
        font_angle = '0';
        chartXOffset = '300px';
        chart_height += (top_values > 10) ? (top_values * 10) : 0;
    } else {
        chart_title = "Top Queried Non-existing Domains";
        chart_height += (top_values > 10) ? (top_values * 7) : 0;
    };

    if (type == "queried_by_ip") {
        var data = dataCsv.split(",");
        var mySeries = [];

        // Clear the arrays when charts are refreshed
        scaleXLabels = [];
        texts = [];
        tooltipTexts = [];

        var values = [];
        var myObj = {
            "values": values,
            "valueBox": {
                "placement": placement,
                "jsRule" : "CustomFn.formatText()",
                "fontColor": fontColor,
            }
        };

        var stackValues = [];
        var myStackObj = {
            "values": stackValues,
            "text": "",
            "valueBox": {
                "placement": placement,
                "text": "",
                "fontColor": fontColor
            }
        };

        // Parse data for the queried by ip chart
        for (var i = 0; i < data.length; i+=4){
            tooltipTexts.unshift(data[i+3]);
            scaleXLabels.unshift(data[i+1]);
            texts.unshift(data[i]);
            values.unshift(parseInt(data[i+2]));

            if (parseInt(data[i+3]) - parseInt(data[i+2]) == 0) {
                var additional = null;
            } else {
                var additional = parseInt(data[i+3]) - parseInt(data[i+2]);
            }
            stackValues.unshift(additional);
        };
        mySeries.push(myObj);
        mySeries.push(myStackObj);
    } else {
        // Parse data for the other charts
        var data = dataCsv.split(",");
        var mySeries = [];
        for (var i = 0; i < data.length; i+=2){
            var myObj = {
                "values": [parseInt(data[i+1])],
                "text": data[i],
            };
            mySeries.push(myObj);
        };
        max_value = parseInt(data[1]);
    }

    // ZingChart configurations
    if (type == "queried_by_ip"){
        var myConfig = {
            type: "hbar",
            plotarea: {
              marginLeft: '120px',
            },
            plot: {
              stacked:true,
              stackType:"normal",
              tooltip:{
                jsRule : "CustomFn.formatTooltip()",
                fontSize: '18',
                padding: "5 10",
                thousandsSeparator: " "
              },
              cursor: 'hand',
            },
            scaleX:{
                visible: "true",
                values: scaleXLabels,
            },
            scaleY:{
                progression: "log",
                minValue: 0,
            },
            title: {
              text: chart_title,
              adjustLayout: true,
              width: "100%",
              paddingBottom: '-15px',
              fontColor:"#444444"
            },
            series: mySeries
        };
    } else if (type == "response_code" || type == "record_type"){
        var myConfig = {
            type: "bar",
            backgroundColor: "#FFFFFF",
            plot: {
              valueBox: {
              rules: [{
                 rule: "%v <= " + max_value/10000,
                 placement: "top-out",
                text: '%t',
                fontAngle: font_angle,
                offsetY: '-15px'
               }, {
                 rule: "%v > " + max_value/10000,
                 placement: placement,
                text: '%t',
                fontAngle: font_angle,
                offsetY: font_offset,
                fontColor: fontColor
               }]
              },
              tooltip:{
                fontSize: '18',
                padding: "5 10",
                text: "%t\n%v",
                thousandsSeparator: " "
              },
              cursor: 'hand'
            },
            scaleX:{
                visible: "false"
            },
            scaleY:{
                progression: "log",
                minValue: 0
            },
            title: {
              text: chart_title,
              adjustLayout: true,
              paddingBottom: '-15px',
              fontColor:"#444444"
            },
            series: mySeries
        };
    } else {
        var myConfig = {
            type: "hbar",
            backgroundColor: "#FFFFFF",
            plot: {
              valueBox: {
                rules: [{
                 rule: "%v < " + max_value/1000,
                 placement: "top-out",
                 text: value_text,
                 fontAngle: font_angle,
                 offsetY: font_offset,
                 fontColor: "black"
               }, {
                 rule: "%v >= " + max_value/1000,
                 placement: placement,
                 text: value_text,
                 fontAngle: font_angle,
                 offsetY: font_offset,
                 fontColor: fontColor
               }]
              },
              tooltip:{
                fontSize: '18',
                padding: "5 10",
                text: tooltip_text,
                thousandsSeparator: " "
              },
              cursor: 'hand',
              animation: {
                      effect:4,
                      sequence: 2,
                      speed: 800,
                      delay: 200
                  }
            },
            scaleX:{
                visible: "false"
            },
            scaleY:{
                progression: "log",
                minValue: 0
            },
            title: {
              text: chart_title,
              adjustLayout: true,
              paddingBottom: '-15px',
              fontColor:"#444444"
            },
            series: mySeries
        };
    }

    // Render ZingChart with hight based on the whole panel
    zingchart.render({
	    id: chartId,
        data : myConfig,
        height: chart_height,
    });
};

window.CustomFn = {};
window.CustomFn.formatText = function(p){
    var tooltipText = texts[p.nodeindex];
    return {
      text : tooltipText,
    }
};

window.CustomFn.formatTooltip = function(p){
    var dataset = zingchart.exec('chart-dns-stats-queried_by_ip', 'getdata');
    var series = dataset.graphset[p.graphindex].series;

    var tooltipText = "";
    var tooltipText = series[0].values[p.nodeindex] + " / " + tooltipTexts[p.nodeindex] + " Records are from IP "
                    + scaleXLabels[p.nodeindex] + "\n";
    return {
      text : tooltipText
    }
};


// Obtain Top N data and generate the chart
function loadTopN(type, number) {
    // Elements ID
    var chartId = '#chart-dns-stats-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide chart element
    $(chartId).hide();
    // Show status element
    $(chartIdStatus).show();

    // Set loading status
    $(chartIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Get filter value (if empty then set "none")
    var filter = $('#filter').val() ? $('#filter').val() : 'none';

    // Set data request
    var data_request = encodeURI( './get_top_n_statistics' + '?beginning=' + beginning + '&end=' + end + '&type=' + type.toLowerCase() + '&number=' + number + '&filter=' + filter);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateTopN(type, response.data, number);
            } else {
                // Show error message
                $(chartIdStatus).html(
                    '<i class="fa fa-exclamation-circle fa-2x"></i>\
                     <span>' + response.status + ': ' + response.data + '</span>'
                )
            }
        }
    });
};


// Load histogram chart, top statistics, and table with all attacks
function loadAllCharts() {
    var topValues = $('#top-values').val();

    loadTopN("record_type", topValues);
    loadTopN("response_code", topValues);
    loadTopN("queried_domain", topValues);
    loadTopN("nonexisting_domain", topValues);
    loadTopN("queried_local", topValues);
    loadTopN("external_dns", topValues);
    loadTopN("queried_by_ip", topValues);
};

// Load all charts when page loaded
$(window).load(loadAllCharts());
