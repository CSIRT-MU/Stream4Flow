// Generate the Top N chart
function generateTopN(type, dataCsv, top_n_value) {
    // Elements ID
    var chartId = 'chart-dns-external-top-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    var max_value = 0;

    if (type == "sources") {
        chart_title = "Top " + top_n_value + " Source IPs using External Resolver";
    } else if (type == "resolvers") {
        chart_title = "Top " + top_n_value + " Queried External DNS Resolvers";
    } else {
        console.log("Error: Chart type: " + type + " is not valid chart type.");
    };

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
        type: "pie",
        backgroundColor: "#FFFFFF",
        plot: {
          valueBox: {
            placement: 'out',
            text: '%t\n%v',
          },
          tooltip:{
            fontSize: '18',
            padding: "5 10",
            text: "%npv%"
          },
          cursor: 'hand'
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
        height: 450,
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
    var chartId = '#chart-dns-external-top-' + type.toLowerCase();
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
    var tableId = '#table-dns-external';
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
    for (var i = 0; i <= array.length-4; i+=4) {
        table.bootstrapTable('insertRow', {
            index: indexCount,
            row: {
                timestamp: array[i],
                src_ip: array[i+1],
                res_ip: array[i+2],
                flows: array[i+3]
            }
        });
        indexCount++;
    }
};

function loadTable() {
    // Elements ID
    var tableId = '#table-dns-external';
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

    loadTopN("sources", topValues);
    loadTopN("resolvers", topValues);
    loadTable();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());
