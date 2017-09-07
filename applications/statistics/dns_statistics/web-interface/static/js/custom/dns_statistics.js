// Global variables for zingchart tooltip and labels formatting in queried by ip statistic
var domains = [];
var total_records_count = [];
var ip_address_labels = [];

// Generate the Top N chart
function generateTopN(type, dataCsv, top_n_value) {
    // Elements ID
    var chartId = 'chart-dns-stats-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    var font_angle = '0';
    var font_offset = '0px';
    var max_value = 0;

    // Set chart height, add additional height if top N value is larger
    var chart_height = (top_n_value > 10) ? (top_n_value * 7 + 450) : 450;

    // Prepare title and unique formatting parameters
    var chart_title = ""
    if (type == "record_type") {
        if (top_n_value > 10) {
            font_offset = '20px';
            font_angle = '-90';
        }
        chart_title = "Top DNS Record Types";
        chart_height = 450;
    } else if (type == "response_code") {
        chart_title = "Top DNS Response Codes";
        chart_height = 450;
    } else if (type == "queried_domain") {
        chart_title = "Top Queried Domains";
    } else if (type == "queried_local") {
        chart_title = "Top Queried Local DNS Servers From Outside Network";
    } else if (type == "external_dns") {
        chart_title = "Top Queried External DNS Servers";
    } else if (type == "queried_by_ip") {
        chart_title = "Device with the most records for domain";
    } else if (type == "nonexisting_domain") {
        chart_title = "Top Queried Non-existing Domains";
    } else {
        console.log("Error: Chart type: " + type + " is not valid chart type.");
    };

    // Prepare variables for parsing data for the charts
    var data = dataCsv.split(",");
    var mySeries = [];

    // Since Queried By Ip statistic provides more amount of data it needs to be parsed differently
    if (type != "queried_by_ip") {
        // Parse data for all charts except queried by ip
        for (var i = 0; i < data.length; i+=2){
            var myObj = {
                "values": [parseInt(data[i+1])],
                "text": data[i],
            };
            mySeries.push(myObj);
        };
        max_value = parseInt(data[1]);
    } else {
        // Parse data for the queried by ip chart

        // Clear the global variables when charts are refreshed
        ip_address_labels = [];
        total_records_count = [];
        domains = [];

        // Create object with values for the base bar chart
        var values = [];
        var myObj = {
            "values": values,
            "valueBox": {
                "placement": "bottom-in",
                "jsRule" : "CustomFn.formatText()",
                "fontColor": "white",
            }
        };

        // Create object with values for the stacked bar chart
        var stackValues = [];
        var myStackObj = {
            "values": stackValues,
            "text": "",
            "valueBox": {
                "placement": "bottom-in",
                "text": "",
                "fontColor": "white"
            }
        };
        for (var i = 0; i < data.length; i+=4){
            total_records_count.unshift(data[i+3]);
            ip_address_labels.unshift(data[i+1]);
            domains.unshift(data[i]);
            values.unshift(parseInt(data[i+2]));

            if (parseInt(data[i+3]) - parseInt(data[i+2]) == 0) {
                stackValues.unshift(null);
            } else {
                stackValues.unshift(parseInt(data[i+3]) - parseInt(data[i+2]));
            }
        };
        mySeries.push(myObj);
        mySeries.push(myStackObj);
    }

    // Initialize chart config
    var myConfig = {}

    // ZingChart configurations
    if (type == "queried_by_ip"){
        myConfig = {
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
                thousandsSeparator: " ",
                backgroundColor: "#373F47"
              },
              cursor: 'hand',
            },
            scaleX:{
                visible: "true",
                values: ip_address_labels,
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
        myConfig = {
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
                 placement: "top-in",
                text: '%t',
                fontAngle: font_angle,
                offsetY: font_offset,
                fontColor: "white"
               }]
              },
              tooltip:{
                fontSize: '18',
                padding: "5 10",
                text: "%t\n%v",
                thousandsSeparator: " ",
                backgroundColor: "#373F47"
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
        myConfig = {
            type: "hbar",
            backgroundColor: "#FFFFFF",
            plot: {
              valueBox: {
                rules: [{
                 rule: "%v < " + max_value/10000,
                 placement: "top-out",
                 text: "%t",
                 fontAngle: font_angle,
                 offsetY: font_offset,
                 fontColor: "black"
               }, {
                 rule: "%v >= " + max_value/10000,
                 placement: "bottom-in",
                 text: "%t",
                 fontAngle: font_angle,
                 offsetY: font_offset,
                 fontColor: "white"
               }]
              },
              tooltip:{
                fontSize: '18',
                padding: "5 10",
                text: "%t\n%v",
                thousandsSeparator: " ",
                backgroundColor: "#373F47"
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

    // Render ZingChart with height based on the selected top n value
    zingchart.render({
	    id: chartId,
        data : myConfig,
        height: chart_height,
    });
};

// Prepares custom functions
window.CustomFn = {};

// Get domain names as values displayed for the stacked bar chart in the queried by ip statistic
window.CustomFn.formatText = function(p){
    var tooltipText = domains[p.nodeindex];
    return {
      text : tooltipText,
    }
};

// Get Tooltip texts for the queried by ip statistic
window.CustomFn.formatTooltip = function(p){
    var dataset = zingchart.exec('chart-dns-stats-queried_by_ip', 'getdata');
    var series = dataset.graphset[p.graphindex].series;

    var tooltipText = "";
    var tooltipText = series[0].values[p.nodeindex] + " / " + total_records_count[p.nodeindex] + " Records are from IP "
                    + ip_address_labels[p.nodeindex] + "\n";
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

    // Parse the n value for statistics
    var top_n_value = parseInt(number);

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Set data request
    var data_request = encodeURI( './get_top_n_statistics' + '?beginning=' + beginning + '&end=' + end + '&type=' + type.toLowerCase() + '&number=' + number);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateTopN(type, response.data, top_n_value);
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


function generateTable(data, type) {
    // Elements ID
    var tableId = '#table-dns-stats';
    var tableIdStatus = tableId + '-status';

    // Hide status element
    $(tableIdStatus).hide();
    // Show table element
    $(tableId).show();

    var indexCount = 0;
    var array = data.split(",");
    var table = $("#table-records");

    // Empty current data in table
    table.bootstrapTable('removeAll');

    // Generate rows for table
    for (var i = 0; i <= array.length-2; i+=2) {
        table.bootstrapTable('insertRow', {
            index: indexCount,
            row: {
                record: array[i],
                count: array[i+1],
            }
        });
        indexCount++;
    }
};

function loadTable() {
    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Gets type and value for selected option
    var type = $('#all-values').val();
    var text = $('#all-values option:selected').text();

    // Change Table title according to the table type
    $('#table-title').html(text);

    var data_request = encodeURI( './get_records_list' + '?beginning=' + beginning + '&end=' + end + '&type=' + type);

    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateTable(response.data);
            } else {
                // Show error message
                $("#table").html(
                    '<i class="fa fa-exclamation-circle fa-2x"></i>\
                     <span>' + response.status + ': ' + response.data + '</span>'
                )
            }
        }
    });

};

function loadAllCharts() {
    // Hide the table element
    $('.table-dns-stats').hide();
    // Show charts
    $('.chart-dns-stats-top').show();
    // Get the top n value
    var topValues = $('#top-values').val();

    // Load charts for all types of statistics
    loadTopN("record_type", topValues);
    loadTopN("response_code", topValues);
    loadTopN("queried_domain", topValues);
    loadTopN("nonexisting_domain", topValues);
    loadTopN("queried_local", topValues);
    loadTopN("external_dns", topValues);
    loadTopN("queried_by_ip", topValues);
};

function loadAllRecords() {
    // Hide charts
    $('.chart-dns-stats-top').hide();
    // Show the table element
    $('.table-dns-stats').show();
    // Load the table with data
    loadTable();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());
