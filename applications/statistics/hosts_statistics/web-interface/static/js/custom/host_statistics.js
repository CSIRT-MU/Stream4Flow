//------------------- Common Functions --------------------------
function setFocus(element_id) {
    $('html, body').animate({
        scrollTop: $(element_id).offset().top
    }, 1000);
};


//------------------- Heatmap Chart --------------------------
// Generate the histogram chart
function generateHeatmap(data) {
    // Heatmap ID
    var chartId = 'chart-host-heatmap';
    var chartIdStatus = chartId + '-status';

    // Get network prefix
    var filter = $('#filter').val().split('.');
    var network_prefix = filter[0] + '.' + filter[1] + '.';

    $('#' + chartId).highcharts({
        data: {
            csv: data
        },
        chart: {
            type: 'heatmap',
            zoomType: 'xy'
        },
        title: {
             text: '',
             style: {
                marginTop: '20px',
            },
        },
        xAxis: {
            type: 'linear',
            title:{
                text: "D segment of IP address"
            },
            tickInterval: 1
        },
        yAxis: {
            type: 'linear',
            title:{
                text: "C segment of IP address"
            },
            tickInterval: 4
        },
        colorAxis: {
            stops: [
                [0, '#ffffff'],
                [0.001, '#3060cf'],
                [0.01, '#c4463a'],
                [0.1, '#c4463a'],
                [1, '#000000']
            ],
            min: 0,
            max: 50000,
            startOnTick: false,
            endOnTick: false,
            labels: {
                format: '{value}'
            }
        },
        exporting: {
            enabled: false
        },
        tooltip: {
            enabled: true
        },
        plotOptions: {
                series: {
                    cursor: 'crosshair',
                    point: {
                        events: {
                            click: function (e) {
                                // Show details about selected host
                                var host_ip = network_prefix + this.y + "." + this.x;
                                loadHostCharts(host_ip);
                            }
                        }
                    },
                    marker: {
                        enabled: true,
                        lineWidth: 1
                    }
                }
        },
        series: [{
            borderWidth: 0,
            nullColor: '#EFEFEF',
            tooltip: {
                headerFormat: 'Host IP Address<br/>',
                pointFormat: network_prefix + '{point.y}.{point.x} – <b>{point.value} flows</b>'
            },
            turboThreshold: Number.MAX_VALUE // #3404, remove after 4.0.5 release
        }]
    });

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();
    // Reflow chart to fit into parent element
    $('#' + chartId).highcharts().reflow();
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
    var data_request = encodeURI( './get_heatmap_statistics' + '?beginning=' + beginning + '&end=' + end + '&filter=' + filter);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                // Replace separator ';' to new line to create a CSV string and generate the heatmap
                generateHeatmap(response.data.replace(/;/g,'\n'));
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
function generateHostFlowsChart(data, host) {
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
            text: 'Traffic Statistics of the Host ' + host,
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
	    height: $('#' + chartId).height()
    });
};

// Obtain flow data for a host and generate the chart
function loadHostFlowsChart(host_ip) {
    // Elements ID
    var chartId = '#chart-host-flows';
    var chartIdStatus = chartId + '-status';
    var chartIdPanel = chartId + '-panel';

    // Show chart panel
    $(chartIdPanel).show();
    // Hide chart element
    $(chartId).hide();
    // Show status element
    $(chartIdStatus).show();

    // Set loading status
    $(chartIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Scroll to this element ID
    setFocus(chartIdPanel);

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Set data request
    var data_request = encodeURI( './get_host_flows' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&host_ip=' + host_ip);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateHostFlowsChart(response.data, host_ip);
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
            text: 'TCP Flags of the Host ' + host,
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
	    height: $('#' + chartId).height()
    });
};

// Obtain flow data for a host and generate the chart
function loadHostTcpChart(host_ip) {
    // Elements ID
    var chartId = '#chart-host-flags';
    var chartIdStatus = chartId + '-status';
    var chartIdPanel = chartId + '-panel';

    // Hide chart element
    $(chartId).hide();
    // Show status element
    $(chartIdStatus).show();
    // Show chart panel
    $(chartIdPanel).show();

    // Set loading status
    $(chartIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Set data request
    var data_request = encodeURI( './get_host_tcp_flags' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&host_ip=' + host_ip);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateHostTcp(response.data, host_ip);
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


//------------------- Host Count of Distinct Dst Ports Chart --------------------------
// Generate a chart and set it to the given div
function generateHostDistinctPorts(data, host) {
    // Elements ID
    var chartId = 'chart-host-distinct-ports';
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // ZingChart configuration
    var myConfig = {
        type: 'mixed',
        backgroundColor:'#fff',
        title:{
            text: 'Distinct Destination Ports for the Host ' + host,
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
            },
            "data-min": data.data_min,
            "data-max": data.data_max,
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
            decimals: 0,
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
              scaleLabel:{
                backgroundColor:"#7CB5EC"
              },
              plotLabel:{
                backgroundColor:"white",
                multiple:false,
                borderRadius:3,
                text: "<span style='color:#29a2cc;font-weight:bold;'>Avg</span>: %v Ports \n<span style='color:#abd1f2;font-weight:bold;'>Range</span>: %data-min - %data-max Ports"
              }
            },
        series:[
          {
	        type:"line",
	        values:data.data_avg,
	        text : "Average",
            lineWidth:1,
            shadow:false,
            lineColor:"#29a2cc",
        	tooltip:{
			  visible:false
			}

          },
    	  {
		  type:"range",
			values : data.data_min_max,
			text: "Min-max range",
			backgroundColor:"#abd1f2",
			lineColor:"#abd1f2",
			lineWidth:0,
			marker:{
			  visible:false
			},
			tooltip:{
			  visible:false
			},
			guideLabel:{
			  visible:false
			}
		}
        ]

    };

    // Render ZingChart with width based on the whole panel
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: $('#' + chartId).height()
    });
};

// Obtain flow data for a host and generate the chart
function loadHostDistinctPorts(host_ip) {
    // Elements ID
    var chartId = '#chart-host-distinct-ports';
    var chartIdStatus = chartId + '-status';
    var chartIdPanel = chartId + '-panel';

    // Hide chart element
    $(chartId).hide();
    // Show status element
    $(chartIdStatus).show();
    // Show chart panel
    $(chartIdPanel).show();

    // Set loading status
    $(chartIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Set data request
    var data_request = encodeURI( './get_host_distinct_ports' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&host_ip=' + host_ip);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateHostDistinctPorts(response.data, host_ip);
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


//------------------- Host Count of Distinct Peers Chart --------------------------
// Generate a chart and set it to the given div
function generateHostDistinctPeers(data, host) {
    // Elements ID
    var chartId = 'chart-host-distinct-peers';
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();


    // ZingChart configuration
    var myConfig = {
        type: 'mixed',
        backgroundColor:'#fff',
        title:{
            text: 'Distinct Peers for the Host ' + host,
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
            },
            "data-min": data.data_min,
            "data-max": data.data_max,
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
            decimals: 0,
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
              scaleLabel:{
                backgroundColor:"#7CB5EC"
              },
              plotLabel:{
                backgroundColor:"white",
                multiple:false,
                borderRadius:3,
                text: "<span style='color:#29a2cc;font-weight:bold;'>Avg</span>: %v Peers \n<span style='color:#abd1f2;font-weight:bold;'>Range</span>: %data-min - %data-max Peers"
              }
            },
        series:[
          {
	        type:"line",
	        values:data.data_avg,
	        text : "Average",
            lineWidth:1,
            shadow:false,
            lineColor:"#29a2cc",
        	tooltip:{
			  visible:false
			}

          },
    	  {
		  type:"range",
			values : data.data_min_max,
			text: "Min-max range",
			backgroundColor:"#abd1f2",
			lineColor:"#abd1f2",
			lineWidth:0,
			marker:{
			  visible:false
			},
			tooltip:{
			  visible:false
			},
			guideLabel:{
			  visible:false
			}
		}
        ]

    };

    // Render ZingChart with width based on the whole panel
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: $('#' + chartId).height()
    });
};

// Obtain flow data for a host and generate the chart
function loadHostDistinctPeers(host_ip) {
    // Elements ID
    var chartId = '#chart-host-distinct-peers';
    var chartIdStatus = chartId + '-status';
    var chartIdPanel = chartId + '-panel';

    // Hide chart element
    $(chartId).hide();
    // Show status element
    $(chartIdStatus).show();
    // Show chart panel
    $(chartIdPanel).show();

    // Set loading status
    $(chartIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Set data request
    var data_request = encodeURI( './get_host_distinct_peers' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&host_ip=' + host_ip);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateHostDistinctPeers(response.data, host_ip);
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


//------------------- Loading functions --------------------------

// Load defined host charts
function loadHostCharts(host_ip) {
    loadHostFlowsChart(host_ip);
    loadHostTcpChart(host_ip);
    loadHostDistinctPorts(host_ip);
    loadHostDistinctPeers(host_ip);
};


// Load all charts when page loaded
$(window).load(loadHeatmapChart());