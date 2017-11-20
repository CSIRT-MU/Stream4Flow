// --------------------- Cluster Overview ---------------------
// Generate Cluster Overview
function generateClusterOverview(data) {
    // Elements ID
    var tableDivId = '#cluster-overview-table';
    var tableDivIdStatus = tableDivId + '-status';

    // Hide status element
    $(tableDivIdStatus).hide();
    // Show table element
    $(tableDivId).show();

    // Set master info
    var masterTable = $('#master-overview');
    // Clear old data
    masterTable.bootstrapTable('removeAll');
    // Append master information
    masterTable.bootstrapTable('insertRow', {
        index: 0,
        row: {
            uiAddress: '<a href="' + data['master']['ui_address'] + '" target="_blank">' + data['master']['ui_address'] + '</a>',
            state: data['master']['state']
        }
    });
    // Check is master is ALIVE
    if (data['master']['state'] != 'ALIVE') {
        // Set background color to warn that master is not working
        $('#master-overview tbody tr').eq(0).addClass('row-error');
    } else {
        // Else set success row background
        $('#master-overview tbody tr').eq(0).addClass('row-ok');

        // Set workers info
        var workersTable = $('#workers-overview');
        // Clear old data
        workersTable.bootstrapTable('removeAll');
        // Iterate through all workers
        var indexCount = 0;
        for (var workerIp in data["workers"]) {
            // Get worker info
            var worker = data["workers"][workerIp];
            var workerId = worker['id'];
            var workerUiAddress = '<a href="' + worker['webuiaddress'] + '" target="_blank">' + worker['webuiaddress'] + '</a>';
            var workerCores = worker['cores'] + ' (' + worker['coresused'] + ' Used)';
            var workerMemory = worker['memory'] + ' (' + worker['memoryused'] + ' Used)';
            var workerState = worker['state'];

            // Check if worker status if ALIVE
            if (workerState != "ALIVE") {
                // Update row content
                  workerId = "IP address " + worker['host'] + " has no ID";
                  workerUiAddress = "";
                  workerCores = "";
                  workerMemory = "";
            };

            // Add row to the table
            workersTable.bootstrapTable('insertRow', {
                index: indexCount,
                row: {
                    workerId: workerId,
                    uiAddress: workerUiAddress,
                    cores: workerCores,
                    memory: workerMemory,
                    state: workerState
                }
            });

            indexCount++;
        };
    };
}

// Load Cluster Overview
function loadClusterOverview() {
    // Elements ID
    var tableDivId = '#cluster-overview-table';
    var tableDivIdStatus = tableDivId + '-status';

    // Hide table element
    $(tableDivId).hide();
    // Show status element
    $(tableDivIdStatus).show();

    // Set loading status
    $(tableDivIdStatus).html(
        '<i class="fa fa-refresh fa-spin fa-2x fa-fw"></i>\
         <span>Loading...</span>'
    )

    // Set data request
    var data_request = encodeURI('./get-cluster-overview');
    // Get cluster overview data
    $.ajax({
        async: true,
        type: 'GET',
        url: data_request,
        success: function(raw) {
            var response = jQuery.parseJSON(raw);
            if (response.status == "Ok") {
                generateClusterOverview(response.data);
            } else {
                // Show error message
                $(tableDivIdStatus).html(
                    '<i class="fa fa-exclamation-circle fa-2x"></i>\
                     <span>' + response.status + ': ' + response.data + '</span>'
                )
            }
        }
    });
};


// --------------------- Loading Functions ---------------------

function loadClusterControl() {
    // Load cluster overview
    loadClusterOverview();
};

// Load all when page loaded
$(window).load(loadClusterControl());
