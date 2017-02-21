// Set interval on select interval change
function setInterval(value) {
    // On "custom" value show datetimepicker for "#beginning"
    if (value == "custom") {
        $('#beginning').datetimepicker('toggle');
        return;
    };

    // Check NaN
    if ( isNaN(value) ) return;

    // Get current datetime with seconds rounded to tens
    var datetime = new Date(new Date().getTime() - new Date().getTimezoneOffset()*60*1000);
    var secondsRounded = parseInt(datetime.getSeconds() / 10) * 10;
    datetime.setSeconds(secondsRounded);

    // Set '#beginning' to current time
    $('#datetime-end').val( datetime.toISOString().substr(0, 19).replace('T', ' ') );

    // Set '#end' to current time subtract given value
    datetime.setHours(datetime.getHours() - parseInt(value));
    $('#datetime-beginning').val( datetime.toISOString().substr(0, 19).replace('T', ' ') );
}

// Set default interval to 2 hours
$(window).load(setInterval(2));
