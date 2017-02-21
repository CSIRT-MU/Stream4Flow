// Generate a chart and set it to the given div
function generateSumChart(data) {
    // Elements ID
    var chartId = 'chart-sum';
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // ZingChart configuration
    var myConfig = {
        type: "bar",
        backgroundColor:'#fff',
        legend:{
            align: 'center',
            verticalAlign: 'bottom',
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
            margin: '15px'
        },
        plot:{
            valueBox:{
                visible: true,
                thousandsSeparator: ','
            },
            animation:{
                effect: 4,
                speed: 1
            },
            barWidth: '66%'
        },
        scaleX:{
            label:{
                visible: false
            }
        },
        scaleY:{
            visible: false,
            progression: 'log',
            logBase: 10,
            minValue: 0
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
	    height: $('#network-statistics').height() - 60,
	    width: $('#network-statistics').width(),
    });
};


// Obtain chart data and generate chart
function loadSumChart() {
    // Elements ID
    var chartId = '#chart-sum';
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

    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: './get-summary-statistics',
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                // Replace separator ';' to new line to create a CSV string and generate a chart
                generateSumChart(response.data);
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


// Load all charts when page loaded
$(window).load(loadSumChart());
