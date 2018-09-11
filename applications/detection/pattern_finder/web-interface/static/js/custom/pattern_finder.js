// Generate the Top N chart
function generateTopN(type, dataCsv) {
    // Elements ID
    var chartId = 'chart-pattern-finder-top-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // Parse data for the chart
    var data = dataCsv.split(",");
    var mySeries = [];
    for (var i = 0; i < data.length; i+=2){
        var myObj = {
            "values": [parseInt(data[i+1])],
            "text": data[i]
        };
        mySeries.push(myObj);
    }

    // ZingChart configuration
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
          text: 'Top ' + type,
          adjustLayout: true,
          paddingBottom: '-15px',
          fontColor:"#444444"
        },
        series: mySeries
    };

    // Render ZingChart with hight based on the whole panel
    zingchart.render({
	    id: chartId,
        data : myConfig,
        height: $('#' + chartId).height()
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
    var chartId = '#chart-pattern-finder-top-' + type.toLowerCase();
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

    // Get filters value (if empty then set "none")
    var filter = $('#filter').val() ? $('#filter').val() : 'none';
    var config_filer = $('#config_filter').val() ? $('#config_filter').val() : 'none';

    // Set data request
    var data_request = encodeURI( './get_top_n_statistics' + '?beginning=' + beginning + '&end=' + end + '&type=' + type.toLowerCase() + '&number=' + number + '&filter=' + filter + '&config_filter=' + config_filer);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateTopN(type, response.data);
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


// Generate the attacks table
function generateTable(dataCsv) {
    // Elements ID
    var tableId = '#table-pattern-finder';
    var tableIdStatus = tableId + '-status';

    // Hide status element
    $(tableIdStatus).hide();
    // Show table element
    $(tableId).show();

    var indexCount = 0;
    var table = $(tableId + ' table')

    // Prepare data
    var array = dataCsv.split(",");
    // Clear the table
    table.bootstrapTable('removeAll');

    // Fill the table
    for (var i = 0; i <= array.length - 6; i += 6) {
        table.bootstrapTable('insertRow', {
            index: indexCount,
            row: {
                timestamp: array[i],
                src_ip: array[i+1],
                dst_ip: array[i+2],
                configuration: array[i+3],
                detections: array[i+4],
                confidence: array[i+5]
            }
        });
        indexCount++;
    }
};


// List all detections
function loadTable() {
    // Elements ID
    var tableId = '#table-pattern-finder';
    var tableIdStatus = tableId + '-status';

    // Hide table element
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

    // Get filters value (if empty then set "none")
    var filter = $('#filter').val() ? $('#filter').val() : 'none';
    var config_filer = $('#config_filter').val() ? $('#config_filter').val() : 'none';

    // Set data request
    var data_request = encodeURI( './get_attacks_list' + '?beginning=' + beginning + '&end=' + end + '&filter=' + filter + '&config_filter=' + config_filer);
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


// Load histogram chart, top statistics, and table with all attacks
function loadAllCharts() {
    loadTopN("Sources", 10);
    loadTopN("Victims", 10);
    loadTable();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());