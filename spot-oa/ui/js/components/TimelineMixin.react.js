const $ = require('jquery');
const d3 = require('d3');
const React = require('react');

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
        this.canvas = d3.select(this.getDOMNode()).select('*').datum(this.state.data);

        const dataDate = this.state.date;
        const startTime = Date.parse(dataDate + " 00:00");
        const endTime = Date.parse(dataDate + " 23:59");

        this.eventDropsChart = d3.chart.eventDrops()
            .start(startTime)
            .end(endTime)
            .locale(locale)
            .axisFormat(xAxis => xAxis.ticks(5))
            .eventLineColor(e => colorScale(e.name));

        if (this.getTooltipContent) {
            // Create a tooltip
            this.tooltip = d3.tip()
                .attr('class', 'd3-tip')
                .html(d => this.getTooltipContent(d));

            this.eventDropsChart.eventHover((e) => {
                const eventData = d3.select(e).data()[0];
                // Get data from event's parent. Super relying on eventDrops implementation
                const parentData = d3.select($(e).parent().get(0)).data()[0];

                // Hide tooltip when mouse leaves event node.
                // d3 will remove the event handler before adding
                // the new one. So good!!!
                /*d3.select(e).on('mouseleave', function () {
                    this.tooltip.hide();
                });
                d3.select($(e).parent().get(0)).on('mouseleave', function () {
                    this.tooltip.hide();
                });*/

                // Show tooltip
                const tooltipData = {
                    context: parentData,
                    date: eventData.toLocaleTimeString()
                };

                this.tooltip.show(tooltipData, e);
            });
        }
    },
    draw() {
        // Get current viewport width
        this.eventDropsChart.width($(this.getDOMNode()).width())

        // Create svg element and draw eventDropsChart
        this.canvas.call(this.eventDropsChart);

        // Add a tooltip
        if (this.getTooltipContent) {
            this.canvas.select('svg').call(this.tooltip);
        }
    }
};

module.exports = TimelineMixin;
