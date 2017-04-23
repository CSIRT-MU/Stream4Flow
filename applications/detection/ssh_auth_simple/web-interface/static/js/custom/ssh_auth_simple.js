// Generate the histogram chart
function generateHistogram(dataJson) {
    // Elements ID
    var chartId = 'chart-ssh-simple-histogram';
    var chartIdStatus = chartId + '-status';

    // Get min and max time value
    var beginning = new Date( $('#datetime-beginning').val()).getTime();
    var end = new Date( $('#datetime-end').val()).getTime();

    // Parse data for the chart
    var mySeries = [];
    for (var source_ip in dataJson) {
        var myObj = {
            "values": dataJson[source_ip],
            "text" : source_ip
        };
        mySeries.push(myObj);
    }

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // ZingChart configuration
    var myConfig = {
        type: 'line',
        backgroundColor:'#fff',
        title:{
            text: 'Attacks in Time by Source',
            adjustLayout: true,
            fontColor:"#444444"
        },
        legend:{
            align: 'center',
            verticalAlign: 'top',
            backgroundColor:'none',
            borderWidth: 0,
            item:{
                fontColor:'#444444',
                cursor: 'hand'
            },
            marker:{
                type:'circle',
                borderWidth: 0,
                cursor: 'hand'
            },
            toggleAction: 'remove',
            maxItems: 8,
            overflow: 'scroll',
            scroll:{
                bar:{
                    border: '1px solid #444444',
                    height: '6'
                },
                handle:{
                    backgroundColor: '#668586'
                }
            }
        },
        plotarea:{
            margin:'dynamic 70'
        },
        plot:{
            lineWidth: 2,
            marker:{
                borderWidth: 0,
                size: 4
            },
            hoverMarker:{
                borderWidth: 2,
                size: 6,
                borderColor: '#000'
            },
            cursor: 'hand'
        },
        scaleX:{
            minValue: beginning,
            maxValue: end,
            lineColor: '#444444',
            zooming: true,
            item:{
                fontColor:'#444444'
            },
            transform:{
                type: 'date',
                all: '%D %M %d<br>%H:%i:%s'
            },
            label:{
                text: 'Time',
                visible: false
            }
        },
        scaleY:{
            progression: "log",
            logBase: 10,
            minorTicks: 1,
            lineColor: '#444444',
            tick:{
                lineColor: '#444444'
            },
            minorTick:{
                lineColor: '#444444'
            },
            minorGuide:{
                visible: false
            },
            guide:{
                lineStyle: 'dashed'
            },
            item:{
                fontColor:'#444444'
            },
            label:{
                text: 'Count',
                fontSize: 12,
                fontColor: '#444444'
            },
            short: true
        },
        tooltip:{
            borderWidth: 1,
            borderRadius: 3,
            text: '<b>%t:</b> %v<br>%k',
            backgroundColor: '#fff',
            borderColor: '#444444',
            fontColor: '#444444',
            callout: true
        },
        preview:{
            adjustLayout: true,
            y: '85%',
            borderColor:'#444444',
            borderWidth: 1,
            mask:{
                backgroundColor:'#658687'
            }
        },
        series: mySeries
    };

    // Render ZingChart with hight based on the whole panel
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: $('#chart-ssh-simple-histogram').height() - 20 // Fixing zingchart watermark
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


// Obtain histogram data and generate the chart
function loadHistogramChart() {
    // Elements ID
    var chartId = '#chart-ssh-simple-histogram';
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
    var data_request = encodeURI( './get_histogram_statistics' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&filter=' + filter);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateHistogram(response.data);
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


// Generate the Top N chart
function generateTopN(type, dataCsv) {
    // Elements ID
    var chartId = 'chart-ssh-simple-top-' + type.toLowerCase();
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
    var chartId = '#chart-ssh-simple-top-' + type.toLowerCase();
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
    var tableId = '#table-ssh-simple';
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
    for (var i = 0; i <= array.length - 5; i += 5) {
        table.bootstrapTable('insertRow', {
            index: indexCount,
            row: {
                timestamp: array[i],
                src_ip: array[i+1],
                dst_ip: array[i+2],
                flows: array[i+3],
                duration: array[i+4]
            }
        });
        indexCount++;
    }
};


// List all detected attacks
function loadTable() {
    // Elements ID
    var tableId = '#table-ssh-simple';
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

    // Get filter value (if empty then set "none")
    var filter = $('#filter').val() ? $('#filter').val() : 'none';

    // Set data request
    var data_request = encodeURI( './get_attacks_list' + '?beginning=' + beginning + '&end=' + end + '&filter=' + filter);
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
    loadHistogramChart();
    loadTopN("Sources", 10);
    loadTopN("Victims", 10);
    loadTable();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());