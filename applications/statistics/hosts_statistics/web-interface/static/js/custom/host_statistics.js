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

// Obtain histogram data and generate the chart
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

// Load histogram chart, top statistics, and table with all attacks
function loadAllCharts() {
    loadHeatmapChart();
    loadHostFlowChart();
};


// Load all charts when page loaded
$(window).load(loadAllCharts());