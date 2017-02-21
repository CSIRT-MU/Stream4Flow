// Reflow zingcharts when main page width is changed
$("#page-wrapper").on('transitionend', function () {
    $('.zingchart').each(function() {
        zingchart.exec(this.id, 'resize', {
	        width: '100%'
        });
    });
});