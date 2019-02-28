// Generate the Top N chart
function generateTopN(type, dataCsv, top_n_value) {
    // Elements ID
    var chartId = 'chart-tls-classification-top-' + type.toLowerCase();
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // Prepare title and unique formatting parameters
    var chart_title = ""
    if (type == "os") {
        chart_title = "Top Operating Systems";
    } else if (type == "browser") {
        chart_title = "Top Browsers";
    } else if (type == "application") {
        chart_title = "Top Applications";
    } else {
        console.log("Error: Chart type: " + type + " is not valid chart type.");
    }

    // Prepare variables for parsing data for the charts
    var data = dataCsv.split(",");
    var mySeries = [];

    for (var i = 0; i < data.length; i+=2){
        // Do not process Unknown values
        if ((data[i] != "Unknown") && (data[i] != "Unknown:Unknown")) {
            var myObj = {
                "values": [parseInt(data[i+1])],
                "text": data[i],
            };
            mySeries.push(myObj);
        }
    };

    // ZingChart configuration
    var myConfig = {
        type: "pie",
        globals: {
            fontFamily: "Roboto Condensed"
        },
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
          }
        },
        title: {
          text: chart_title,
          adjustLayout: true,
          paddingBottom: '-30px',
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
}

function generateTopNsApplication(dataCsv, top_n_value) {
    // Elements ID
    var chartId = 'chart-tls-classification-top-application';
    var chartIdStatus = chartId + '-status';

    // Hide status element
    $('#' + chartIdStatus).hide();
    // Show chart element
    $('#' + chartId).show();

    // Prepare variables for parsing data for the charts
    var data = dataCsv.split(",");
    var mySeriesMobile = [];
    var mySeriesDesktop = [];
    var mySeriesUnknown = [];

    for (var i = 0; i < data.length; i+=2){
        if ((data[i] != "Unknown") && (data[i] != "Unknown:Unknown")) {
            var type = data[i].split(":");
            var myObj = {
                "values": [parseInt(data[i+1])],
                "text": type[1],
            };
            if (type[0] == "Mobile") {
                mySeriesMobile.push(myObj);
            } else if (type[0] == "Desktop") {
                mySeriesDesktop.push(myObj);
            } else {
                mySeriesUnknown.push(myObj);
            }
        }
    };

    // ZingChart configuration
    var myConfig = {
        globals: {
            fontFamily: "Roboto Condensed"
        },
        layout: "h",
        graphset: [
            {
                type: "pie",
                backgroundColor: "transparent",
                plot: {
                  size: 100,
                  slice: 65,
                  valueBox: {
                    placement: 'out',
                    text: '%t\n%v',
                  },
                  tooltip:{
                    fontSize: '18',
                    padding: "5 10",
                    text: "%npv%"
                  }
                },
                labels: [{
                    x: "45%",
                    y: "50%",
                    width: "10%",
                    text: "Desktop",
                    fontWeight: "bold",
                    fontColor: "#373f47",
                    fontSize: 20
                }],
                series: mySeriesDesktop
            },
            {
                type: "pie",
                backgroundColor: "transparent",
                plot: {
                  size: 100,
                  slice: 65,
                  valueBox: {
                    placement: 'out',
                    text: '%t\n%v',
                  },
                  tooltip:{
                    fontSize: '18',
                    padding: "5 10",
                    text: "%npv%"
                  }
                },
                labels: [{
                    x: "45%",
                    y: "50%",
                    width: "10%",
                    text: "Mobile",
                    fontWeight: "bold",
                    fontColor: "#373f47",
                    fontSize: 20
                }],
                series: mySeriesMobile
            },
            {
                type: "pie",
                backgroundColor: "transparent",
                plot: {
                  size: 100,
                  slice: 65,
                  valueBox: {
                    placement: 'out',
                    text: '%t\n%v',
                  },
                  tooltip:{
                    fontSize: '18',
                    padding: "5 10",
                    text: "%npv%"
                  }
                },
                labels: [{
                    x: "45%",
                    y: "50%",
                    width: "10%",
                    text: "Unknown",
                    fontWeight: "bold",
                    fontColor: "#373f47",
                    fontSize: 20
                }],
                series: mySeriesUnknown,
                title: {}
            }
        ]
    };

    // Render ZingChart with hight based on the whole panel
    zingchart.render({
	    id: chartId,
        data : myConfig,
        height: $('#' + chartId).height()
    });
}


// Obtain Top N data and generate the chart
function loadTopN(type, number) {
    // Elements ID
    var chartId = '#chart-tls-classification-top-' + type.toLowerCase();
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

    // Set data request
    var data_request = encodeURI( './get_top_n_statistics' + '?beginning=' + beginning + '&end=' + end + '&type=' + type.toLowerCase() + '&number=' + number);
    // Get Elasticsearch data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                if (type == "application") {
                    generateTopNsApplication(response.data, top_n_value);
                } else {
                    generateTopN(type, response.data, top_n_value);
                }
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


function loadAllCharts() {
    // Show charts
    $('.chart-dns-stats-top').show();
    // Get the top n value
    var topValues = $('#top-values').val();

    // Load charts for all types of statistics
    loadTopN("os", topValues);
    loadTopN("browser", topValues);
    loadTopN("application", topValues);
};


// Load all charts when page loaded
$(window).load(loadAllCharts());
