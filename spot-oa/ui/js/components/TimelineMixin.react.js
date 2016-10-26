var $ = require('jquery');
var d3 = require('d3');
var React = require('react');

const colorScale = d3.scale.category10();

const locale = d3.locale({
    "decimal": ",",
    "thousands": " ",
    "grouping": [3],
    "dateTime": "%A %e %B %Y, %X",
    "date": "%d/%m/%Y",
    "time": "%H:%M:%S",
    "periods": ["AM", "PM"],
    "days": ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
    "shortDays": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
    "months": ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
    "shortMonths": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
});

const TimelineMixin = {
    buildChart() {
        this.canvas = d3.select(this.getDOMNode()).select('div')
                .datum(this.state.data);

        const dataDate = this.state.date;
        const startTime = Date.parse(dataDate + " 00:00");
        const endTime = Date.parse(dataDate + " 23:59");

        this.eventDropsChart = d3.chart.eventDrops()
            .start(new Date(startTime))
            .end(new Date(endTime))
            .locale(locale)
            .eventColor(e => colorScale(e.name))
            .labelsWidth(100)
            .axisFormat(xAxis => xAxis.ticks(5))
            .date(e => new Date(e.date));

        if (this.getTooltipContent) {
            // Create a tooltip
            this.tooltip = d3.tip()
                .attr('class', 'd3-tip')
                .html(e => this.getTooltipContent(e));

            this.eventDropsChart.mouseover(this.tooltip.show);
            this.eventDropsChart.mouseout(this.tooltip.hide);
        }
    },
    draw() {
        // Create svg element and draw eventDropsChart
        this.canvas.call(this.eventDropsChart);

        // Add a tooltip
        if (this.getTooltipContent) {
            this.canvas.select('svg').call(this.tooltip);
        }
    }
};

module.exports = TimelineMixin;
