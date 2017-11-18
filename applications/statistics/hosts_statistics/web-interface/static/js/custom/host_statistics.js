//------------------- Common Functions --------------------------
// Hack necessary for ZingChart (it is unable to work with ID containing numbers)
function replaceNumbers(string) {
    return string.replace(/1/g, 'a').replace(/2/g, 'b').replace(/3/g, 'c').replace(/4/g, 'd').replace(/5/g, 'e').replace(/6/g, 'f').replace(/7/g, 'g').replace(/8/g, 'h').replace(/9/g, 'i')
};

var loaded_hosts = []
function isHostLoaded(host_ip) {
    return ((jQuery.inArray(host_ip, loaded_hosts) >= 0) ? true : false);
};
function addLoadedHost(host_ip) {
    loaded_hosts.push(host_ip);
};
function removeLoadedHost(host_ip) {
   loaded_hosts.splice($.inArray(host_ip, loaded_hosts), 1);
};

function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays * 24 * 60 * 60 * 1000));
    var expires = "expires="+d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
};

function getCookie(cname) {
    var name = cname + "=";
    var ca = document.cookie.split(';');
    for(var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == ' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) == 0) {
            return c.substring(name.length, c.length);
        }
    }
    return "";
};

function closePanel(host_ip) {
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Get id of element to remove
    var element_id = '#' + host_id + '-charts-panel';

    // Remove specified panel
    $(element_id).remove();

    // Remove host IP from loaded IPs
    removeLoadedHost(host_ip);
};


//------------------- Heatmap Chart --------------------------
// Generate the histogram chart
function generateHeatmap(data) {
    // Heatmap ID
    var chartId = 'chart-host-heatmap';
    var chartIdStatus = chartId + '-status';

    // Get network prefix
    var network = $('#network').val().split('.');
    var network_prefix = network[0] + '.' + network[1] + '.';

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

    // Clear loaded hosts
    $('#detailed-charts').empty();
    loaded_hosts = [];

    // Check if network is set
    if ($('#network').val()) {
        // Save network to the cookie
        setCookie("host_statistics-network", $('#network').val(), 365)
    } else {
        // Check if network is stored in cookie
        var network = getCookie("host_statistics-network");
        if (network != "") {
            // Set network stored in cookie and continue
            $('#network').val(network);
        } else {
            // If network is not set and not stored in cookie then show warning message
            $(chartIdStatus).html(
                '<i class="fa fa-exclamation-circle fa-2x"></i>\
                 <span>Network is not set!</span>'
            )

            // Do not generate the heatmap
            return;
        }
    }

    // Set loading status
    $(chartIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Convert times to UTC in ISO format
    var beginning = new Date( $('#datetime-beginning').val()).toISOString();
    var end = new Date( $('#datetime-end').val()).toISOString();

    // Get network value (if empty then set "none")
    var network = $('#network').val() ? $('#network').val() : 'none';

    // Set data request
    var data_request = encodeURI( './get_heatmap_statistics' + '?beginning=' + beginning + '&end=' + end + '&network=' + network);
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
function generateHostFlowsChart(data, host_ip) {
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = host_id + '-flows';
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
            text: 'Traffic Statistics of the Host ' + host_ip,
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
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = '#' + host_id + '-flows';
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
function generateHostTcp(data, host_ip) {
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = host_id + '-tcp-flags';
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
            text: 'TCP Flags of the Host ' + host_ip,
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
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = '#' + host_id + '-tcp-flags';
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
function generateHostDistinctPorts(data, host_ip) {
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = host_id + '-distinct-ports';
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
            text: 'Distinct Destination Ports for the Host ' + host_ip,
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
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = '#' + host_id + '-distinct-ports';
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
function generateHostDistinctPeers(data, host_ip) {
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = host_id + '-distinct-peers';
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
            text: 'Distinct Peers for the Host ' + host_ip,
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
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var host_id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Elements ID
    var chartId = '#' + host_id + '-distinct-peers';
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

// Get chart panel template
function getPanelTemplate(host_ip) {
    // Replace dots in host_ip to dashes to allow its easy use in element ID
    var id = replaceNumbers(host_ip.replace(/\./g, '-'));

    // Define charts panel template
    var panel_template = ' \
        <div id="' + id +'-charts-panel" class="panel-info widget-shadow general charts-detailed"> \
            <!-- Title --> \
            <h4 class="title2"> \
                Detailed statistics for ' + host_ip + ' \
                <a class="close-detailed-panel" onclick="closePanel(\'' + host_ip + '\');false;"> \
                    <i class="fa fa-times-circle" aria-hidden="true"></i> Close \
                </a> \
            </h4> \
            <!-- Statistics selection --> \
            <ul class="chart-type-selector nav nav-tabs" role="tablist"> \
                <li role="presentation" class="active"> \
                    <a href="#' + id +'-flows-panel" id="' + id +'-flows-tab" role="tab" data-toggle="tab" aria-controls="' + id +'-flows-panel" aria-expanded="true">Flows</a> \
                </li> \
                <li role="presentation" class=""> \
                    <a href="#' + id +'-tcp-flags-panel" id="' + id +'-tcp-flags-tab" role="tab" data-toggle="tab" aria-controls="' + id +'-tcp-flags-panel" aria-expanded="false">TCP Flags</a> \
                </li> \
                <li role="presentation" class=""> \
                    <a href="#' + id +'-distinct-ports-panel" id="' + id +'-distinct-ports-tab" role="tab" data-toggle="tab" aria-controls="' + id +'-distinct-ports-panel" aria-expanded="false">Distinct Ports</a> \
                </li> \
                <li role="presentation" class=""> \
                    <a href="#' + id +'-distinct-peers-panel" id="' + id +'-distinct-peers-tab" role="tab" data-toggle="tab" aria-controls="' + id +'-distinct-peers-panel" aria-expanded="false">Distinct Peers</a> \
                </li> \
            </ul> \
            <!-- Charts --> \
            <div class="tab-content scrollbar1"> \
                <!-- Flows --> \
                <div id="' + id +'-flows-panel" class="tab-pane fade active in" role="tabpanel" aria-labelledby="' + id +'-flows-tab"> \
                    <!-- Status --> \
                    <div id="' + id +'-flows-status" class="chart-status"></div> \
                    <!-- Main Chart --> \
                    <div id="' + id +'-flows" class="zingchart"></div> \
                </div> \
                <!-- TCP Flags --> \
                <div id="' + id +'-tcp-flags-panel" class="tab-pane fade" role="tabpanel" aria-labelledby="' + id +'-tcp-flags-tab"> \
                    <!-- Status --> \
                    <div id="' + id +'-tcp-flags-status" class="chart-status"></div> \
                    <!-- Main Chart --> \
                    <div id="' + id +'-tcp-flags" class="zingchart"></div> \
                </div> \
                <!-- Distinct Ports --> \
                <div id="' + id +'-distinct-ports-panel" class="tab-pane fade" role="tabpanel" aria-labelledby="' + id +'-distinct-ports-tab"> \
                    <!-- Status --> \
                    <div id="' + id +'-distinct-ports-status" class="chart-status"></div> \
                    <!-- Main Chart --> \
                    <div id="' + id +'-distinct-ports" class="zingchart"></div> \
                </div> \
                <!-- Distinct Peers --> \
                <div id="' + id +'-distinct-peers-panel" class="tab-pane fade" role="tabpanel" aria-labelledby="' + id +'-distinct-peers-tab"> \
                    <!-- Status --> \
                    <div id="' + id +'-distinct-peers-status" class="chart-status"></div> \
                    <!-- Main Chart --> \
                    <div id="' + id +'-distinct-peers" class="zingchart"></div> \
                </div> \
            </div> \
        </div> \
    '

    // Return generated template
    return panel_template
}

// Load defined host charts
function loadHostCharts(host_ip) {
    // Check if host is already loaded
    if (!isHostLoaded(host_ip)) {
        // Get a new panel template
        var panel_template = $.parseHTML(getPanelTemplate(host_ip));

        // Append panel to the current list
        $('#detailed-charts').append(panel_template);

        // Load flows chart
        loadHostFlowsChart(host_ip);
        // Load TCP Flags charts
        loadHostTcpChart(host_ip);
        // Load distinct communicating ports chart
        loadHostDistinctPorts(host_ip);
        // Load distinct communicating peers chart
        loadHostDistinctPeers(host_ip);

        // Add host to loaded hosts
        addLoadedHost(host_ip);
    }
};


// Load all charts when page loaded
$(window).load(loadHeatmapChart());
