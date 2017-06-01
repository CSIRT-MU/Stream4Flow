//------------------- Histogram Chart --------------------------
// Generate the histogram chart
function generateHistogram(dataJson) {
    // Elements ID
    var chartId = 'chart-host-heatmap';
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

      "graphset": [{
        "type": "piano",
        "theme": "classic",
        "title": {
          "text": "Heatmap",
          "background-color": "none",
          "font-color": "#05636c",
          "font-size": "24px",
          "adjust-layout": true,
          "padding-bottom": 25
        },
        "backgroundColor": "#fff",
        "plotarea": {
          "margin": "dynamic"
        },
        "scaleX": {
          "placement": "opposite",
          "lineWidth": 0,
          "item": {
            "border-color": "none",
            "size": "13px",
            "font-color": "#05636c"
          },
          "guide": {
            "visible": false
          },
          "tick": {
            "visible": false
          },
          "zooming": true,
          "zoom-snap": true,

        },
        "zoom": {
          "preserve-zoom": true,
          "background-color": "#e5e8ea",
          "border-color": "#009",
          "border-width": 2,
          "alpha": 0.75
        },
        "scroll-x": {
          "bar": {
            "border-radius": 3,
            "background-color": "#01579B",
            "alpha": .5
          },
          "handle": {
            "border-radius": 5,
            "background-color": "#01579B",
            "border-top": "none",
            "border-right": "none",
            "border-bottom": "none",
            "border-left": "none"
          }
        },
        "scroll-y": {
          "bar": {
            "border-radius": 3,
            "background-color": "#01579B",
            "alpha": .5
          },
          "handle": {
            "border-radius": 5,
            "background-color": "#01579B",
            "border-top": "none",
            "border-right": "none",
            "border-bottom": "none",
            "border-left": "none"
          }
        },
        "scaleY": {
          "zooming": true,
          "lineWidth": 0,
          "mirrored": true,
          "tick": {
            "visible": false
          },
          "guide": {
            "visible": false
          },
          "item": {
            "border-color": "none",
            "size": "13px",
            "font-color": "#05636c"
          }
        },
        "plot": {
          "aspect": "none",
          "borderWidth": 2,
          "borderColor": "#eeeeee",
          "borderRadius": 7,
          "tooltip": {
            "font-size": "14px",
            "font-color": "white",
            "text": " The surf will be about %v feet.",
            "text-align": "left"
          }/*,
          "rules": [{
            "rule": "%node-value > 6",
            "backgroundColor": "#081D58",
            "font-color": "#05636c"
          }, {
            "rule": "%node-value > 4 && %node-value <= 5",
            "backgroundColor": "#253494",
            "font-color": "#05636c"
          }, {
            "rule": "%node-value > 3 && %node-value <= 4",
            "backgroundColor": "#225EA8",
            "font-color": "#05636c"
          }, {
            "rule": "%node-value > 2 && %node-value <= 3",
            "backgroundColor": "#1D91C0",
            "font-color": "#05636c"
          }, {
            "rule": "%node-value > 1 && %node-value <= 2",
            "backgroundColor": "#41B6C4",
            "font-color": "#05636c"
          }, {
            "rule": "%node-value > 0 && %node-value <= 1",
            "backgroundColor": "#7FCDBB",
            "font-color": "#05636c"
          }]*/
        },
        "series": mySeries
      }]
    };


    // Render ZingChart with hight based on the whole panel
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: $('#chart-host-heatmap').height() - 20 // Fixing zingchart watermark
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
function loadHeatmapChart() {
    // Elements ID
    var chartId = '#chart-host-heatmap';
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
    var data_request = encodeURI( './get_heatmap_statistics' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&filter=' + filter);
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

//------------------- Host Network Traffic Chart --------------------------
// Generate a chart and set it to the given div
function generateChart(data, host) {
    // Elements ID
    var chartId = 'chart-host-flows';
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // ZingChart configuration
    var myConfig = {
        type: 'line',
        backgroundColor:'#fff',
        title:{
            text: 'Network traffic of a host ' + host,
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
            toggleAction: 'remove'
        },
        plotarea:{
            margin:'dynamic 70'
        },
        plot:{
            lineWidth: 2,
            marker:{
                borderWidth: 0,
                size: 3
            }
        },
        scaleX:{
            lineColor: '#444444',
            zooming: true,
            item:{
                fontColor:'#444444'
            },
            transform:{
                type: 'date',
                all: '%D %M %d<br>%h:%i:%s'
            },
            label:{
                text: 'Time',
                visible: false
            }
        },
        scaleY:{
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
            short: true
        },
        tooltip:{
            borderWidth: 0,
            borderRadius: 3
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
        crosshairX:{
            plotLabel:{
                multiple: true,
                borderRadius: 3
            },
            scaleLabel:{
                backgroundColor:'#373f47',
                borderRadius: 3
            },
            marker:{
                size: 7,
                alpha: 0.5
            }
        },
        csv:{
            dataString: data,
            rowSeparator: ';',
            separator: ',',
            verticalLabels: true
        }
    };

    // Render ZingChart with width based on the whole panel
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: $('#chart-panels').height(),
	    width: $('#chart-panels').width()
    });
};

// Obtain flow data for a host and generate the chart
function loadHostFlowChart() {
    // Elements ID
    var chartId = '#chart-host-flows';
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
    var data_request = encodeURI( './get_host_flows' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&filter=' + filter);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateChart(response.data, response.host);
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

//------------------- Host Tcp Chart --------------------------
// Generate a chart and set it to the given div
function generateHostTcp(data, host) {
    // Elements ID
    var chartId = 'chart-host-flags';
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // ZingChart configuration
    var myConfig = {
        type: 'line',
        backgroundColor:'#fff',
        title:{
            text: 'TCP flags of a host ' + host,
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
            toggleAction: 'remove'
        },
        plotarea:{
            margin:'dynamic 70'
        },
        plot:{
            lineWidth: 2,
            marker:{
                borderWidth: 0,
                size: 3
            }
        },
        scaleX:{
            lineColor: '#444444',
            zooming: true,
            item:{
                fontColor:'#444444'
            },
            transform:{
                type: 'date',
                all: '%D %M %d<br>%h:%i:%s'
            },
            label:{
                text: 'Time',
                visible: false
            }
        },
        scaleY:{
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
            short: true
        },
        tooltip:{
            borderWidth: 0,
            borderRadius: 3
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
        crosshairX:{
            plotLabel:{
                multiple: true,
                borderRadius: 3
            },
            scaleLabel:{
                backgroundColor:'#373f47',
                borderRadius: 3
            },
            marker:{
                size: 7,
                alpha: 0.5
            }
        },
        csv:{
            dataString: data,
            rowSeparator: ';',
            separator: ',',
            verticalLabels: true
        }
    };

    // Render ZingChart with width based on the whole panel
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: $('#chart-panels').height(),
	    width: $('#chart-panels').width()
    });
};

// Obtain flow data for a host and generate the chart
function loadHostTcpChart() {
    // Elements ID
    var chartId = '#chart-host-flows';
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
    var data_request = encodeURI( './get_host_tcp_flags' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&filter=' + filter);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateHostTcp(response.data, response.host);
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
    loadHeatmapChart();
    loadHostFlowChart();
    loadHostTcpChart();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());