// Generate a chart and set it to the given div
function generateChart(data_type, data) {
    // Elements ID
    var chartId = 'chart-' + data_type;
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
          text: 'Number of ' + data_type.charAt(0).toUpperCase() +  data_type.slice(1) + ' per Protocol',
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
          label:{
            text: 'Number of Flows',
            fontSize: 12,
            fontColor: '#444444'
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
        csv: {
            dataString: data,
            rowSeparator: ';',
            separator: ',',
            verticalLabels: true
        }
    };

    // Render ZingChart
    zingchart.render({
	    id: chartId,
	    data: myConfig,
	    height: '540px',
	    width: '100%'
    });
};


// Obtain chart data and generate chart
function loadChart(data_type) {
    // Elements ID
    var chartId = '#chart-' + data_type;
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
    var data_request = encodeURI( './get-statistics' + '?beginning=' + beginning + '&end=' + end + '&aggregation=' + $('#aggregation').val() + '&type=' + data_type);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                // Replace separator ';' to new line to create a CSV string and generate a chart
                generateChart(data_type, response.data);
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


// Load flows, packets, and bytes chart
function loadAllCharts() {
    loadChart("flows");
    loadChart("packets");
    loadChart("bytes");
};

// Load all charts when page loaded
$(window).load(loadAllCharts());

// Resize chart to full width after the panel is selected
$('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
    // Derive chart ID
    var chartId = $(e.target).attr("href").replace(/-panel/g,'').slice(1);

    // Resize selcted chart
    zingchart.exec(chartId, 'resize', {
        width : '100%'
    });
});
