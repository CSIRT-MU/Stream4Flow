// Generate the Top N chart
function generateTopN(type, dataCsv, top_n_value) {
    // Elements ID
    var chartId = 'chart-dns-open-top-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    var max_value = 0;

    if (type == "resolved") {
        chart_title = "Top Open Resolver IPs";
    } else if (type == "resolvers") {
        chart_title = "Top Queried Data for Open Resolvers";
    } else {
        console.log("Error: Chart type: " + type + " is not valid chart type.");
    };

    // Set chart height, add additional height if top N value is larger
    var chart_height = (top_n_value > 10) ? (top_n_value * 7 + 450) : 450;

    // Prepare variables for parsing data for the charts
    var data = dataCsv.split(",");
    var mySeries = [];

    // Parse data for all charts except queried by ip
    for (var i = 0; i < data.length; i+=2){
         var myObj = {
         "values": [parseInt(data[i+1])],
         "text": data[i],
         };
         mySeries.push(myObj);
    };
    max_value = parseInt(data[1]);

    // Initialize chart config
    var myConfig = {
            type: "hbar",
            backgroundColor: "#FFFFFF",
            plot: {
              valueBox: {
                rules: [{
                 rule: "%v < " + 10,
                 placement: "top-out",
                 text: "%t",
                 fontColor: "black"
               }, {
                 rule: "%v >= " + 10,
                 placement: "bottom-in",
                 text: "%t",
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
    // Render ZingChart with height based on the selected top n value
    zingchart.render({
	    id: chartId,
        data : myConfig,
        height: chart_height,
    });

    // And selected IP to the filter on click
    zingchart.bind(chartId,'node_click',function(event){
        // Get text of the clicked node
        var plotInfo = zingchart.exec(event.id, 'getobjectinfo',{
            object: 'plot',
            plotindex: event.plotindex
        });
        var ip = plotInfo.text;

        // Set IP to the filter value
        $('#filter').val(ip);
    });
};


// Obtain Top N data and generate the chart
function loadTopN(type, number) {
    // Elements ID
    var chartId = '#chart-dns-open-top-' + type.toLowerCase();
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

    // Get filter value (if empty then set "none")
    var filter = $('#filter').val() ? $('#filter').val() : 'none';

    // Set data request
    var data_request = encodeURI( './get_top_n_statistics' + '?beginning=' + beginning + '&end=' + end + '&type=' + type + '&number=' + number + '&filter=' + filter);
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
    var tableId = '#table-dns-open';
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
    for (var i = 0; i <= array.length-5; i+=6) {
        table.bootstrapTable('insertRow', {
            index: indexCount,
            row: {
                res_ip: array[i],
                top_resolved_query_for_ip: array[i+1],
                top_resolved_data_for_ip: array[i+2],
                top_resolved_count: array[i+3],
                timestamp: array[i+4],
                flows: array[i+5]
            }
        });
        indexCount++;
    }
};

function loadTable() {
    // Elements ID
    var tableId = '#table-dns-open';
    var tableIdStatus = tableId + '-status';

    // Hide chart element
    $(tableId).hide();
    // Show status element
    $(tableIdStatus).show();

    // Set loading status
    $(tableIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Get filter value (if empty then set "none")
    var filter = $('#filter').val() ? $('#filter').val() : 'none';

    var data_request = encodeURI( './get_records_list' + '?beginning=' + beginning + '&end=' + end + '&filter=' + filter);

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
                $(tableIdStatus).html(
                    '<i class="fa fa-exclamation-circle fa-2x"></i>\
                     <span>' + response.status + ': ' + response.data + '</span>'
                )
            }
        }
    });

};

function loadAllCharts() {
    // Get the top n value
    var topValues = $('#top-values').val();

    loadTopN("resolved", topValues);
    loadTopN("resolvers", topValues);
    loadTable();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());
