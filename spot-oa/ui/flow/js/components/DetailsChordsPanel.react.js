const React = require('react');
const ReactDOM = require('react-dom');
const d3 = require('d3');
const ChordsDiagramStore = require('../stores/ChordsDiagramStore');

function buildTooltip (d, input, output) {
    const p = d3.format(".4%");

    var tooltip;

    tooltip = `<h5><strong>${d.gname}</strong></h5>`;
    tooltip+= `<p>Got ${numberFormat(d.gvalue)} bytes. `;
    tooltip+= `${p(d.gvalue / d.mtotal)} of matrix total (${numberFormat(d.mtotal)})</p>`;

    var toInfo = '', fromInfo = '';

    input.forEach((bytes, i) => {
        if (bytes==0) return;

        fromInfo += `<li>${numberFormat(bytes)} bytes from ${this.state.data.map[i]}</li>`
    });
    output.forEach((bytes, i) => {
        if (bytes==0) return;

        toInfo += `<li>${numberFormat(bytes)} bytes to ${this.state.data.map[i]}</li>`
    });

    fromInfo.length && (tooltip+= `<h5><strong>In</strong></h5><ul>${fromInfo}</ul>`);
    toInfo.length && (tooltip+= `<h5><strong>Out</strong></h5><ul>${toInfo}</ul>`);

    return tooltip;
}

const ContentLoaderMixin = require('../../../js/components/ContentLoaderMixin.react');
const ChartMixin = require('../../../js/components/ChartMixin.react');

const colorScale = d3.scale.category20();
const numberFormat = d3.format(".3s");

const DetailsChordsPanel = React.createClass({
    mixins: [ContentLoaderMixin, ChartMixin],
    componentDidMount()
    {
        ChordsDiagramStore.addChangeDataListener(this._onChange);
    },
    componentWillUnmount()
    {
        ChordsDiagramStore.removeChangeDataListener(this._onChange);
    },
    buildChart() {
        // generate chord layout
        this.chord = d3.layout.chord()
            .padding(.05)
            .sortSubgroups(d3.descending)
            .matrix(this.state.data.matrix);

        const dragB = d3.behavior.drag()
                        .on('drag', this.drag);

        // Main SVG
        this.svgSel = d3.select(this.svg)
                        .attr('width', '100%')
                        .attr('height', '100%');

        this.middle = this.svgSel.append('g');
        this.canvas = this.svgSel.append('g')
                            .call(dragB);

        this.tooltip = d3.tip()
            .attr('class', 'd3-tip')
            .html(({d, i}) => {
                const ibytes = this.state.data.matrix[i];
                const obytes = this.state.data.matrix.map(row => row[i]);

                return buildTooltip.call(this, this.state.data.rdr(d), ibytes, obytes);
            });

        this.svgSel.call(this.tooltip);
    },
    draw() {
        const $svg = $(this.svg);

        // Graph dimensions
        const width = $svg.width();
        const height = $svg.height();

        this.middle.attr('transform', `translate(${width/2},${height/2})`);
        this.canvas.attr('transform', `translate(${width/2},${height/2})`);

        const innerRadius = Math.min(width, height) * .41; //.41 is a magic number for graph stilyng purposes
        const outerRadius = innerRadius * 1.1; //1.1 is a magic number for graph stilyng purposes

        // Tooltip metadata
        this.tooltip.target = [width/2, height/2];

        this.drawGroups(this.chord.groups(), innerRadius, outerRadius);
        this.drawChords(this.chord.chords(), innerRadius);
    },
    drawGroups(groups, innerRadius, outerRadius) {
        // Appending the chord paths
        const groupsSel = {};

        groupsSel.update = this.canvas.selectAll('g.group').data(groups);
        groupsSel.enter = groupsSel.update.enter();

        const allGroups = groupsSel.enter.append('g')
                            .classed('group', true)
                            .on('mouseover', (d, i) => {
                                const mouseToTheRight = d3.event.layerX>this.tooltip.target[0];
                                const mouseTotheBottom = d3.event.layerY>this.tooltip.target[1];

                                // Decide where should the tooltip be displayed?
                                //var direction = mouseTotheBottom ? 'n' : 's';
                                var direction = mouseToTheRight ? 'w' : 'e';

                                this.tooltip.direction(direction);

                                this.tooltip.show({d, i}, this.middle.node());

                                this.fade(0.1, i);
                            })
                            .on('mouseout', (d, i) => {
                                this.tooltip.hide();

                                this.fade(1, i);
                            });

        allGroups.append('path')
                   .style('stroke', 'black')
                   .style('fill', d => colorScale(d.index))
                   .style('cursor', 'pointer')
                   .attr('d', d3.svg.arc().innerRadius(innerRadius).outerRadius(outerRadius));

        const visibleGroups = allGroups.filter((d) => {
            const _d = this.state.data.rdr(d);

            // 1. Display every ip when they are 10 or less
            // 2. Always display current threat
            // 3. Filter ips with less than 0.5%
            return (
                this.state.data.matrix.length <= 10
                || _d.gname == this.state.data.ip
                || _d.gvalue / _d.mtotal > 0.005
            );
        });

        visibleGroups.append('text')
                  .each((d) => { d.angle = (d.startAngle + d.endAngle) / 2; })
                  .attr('dy', '.35em')
                  .style('font-family', 'helvetica, arial, sans-serif')
                  .style('font-size', '12px')
                  .style('cursor', 'pointer')
                  .style('font-weight', (d) => {
                      const _d = this.state.data.rdr(d);
                      if (_d.gname == this.state.data.ip) {
                          return '900';
                      }
                      return 'normal';
                  })
                  .attr('text-anchor', (d) => d.angle > Math.PI ? 'end' : null)
                  .attr('transform', (d) => {
                      return 'rotate(' + (d.angle * 180 / Math.PI - 90) + ')'
                          + 'translate(' + (innerRadius * 1.20) + ')'
                          + (d.angle > Math.PI ? 'rotate(180)' : '');
                  })
                  .text((d) => {
                      return this.state.data.rdr(d).gname;
                  });

        visibleGroups.call((selection) => {
            const matrix = this.state.data.matrix;
            selection.each(function (d) {
                const g = d3.select(this).append('g').attr('transform', (d) => {
                    return 'rotate(' + (d.angle * 180 / Math.PI - 90) + ')'
                        + 'translate(' + (innerRadius * 1.15) + ')'
                        + (d.angle > Math.PI ? 'rotate(180)' : '');
                });

                const output = d.value>0;
                const input = matrix.some(row => {
                    return row[d.index]>0;
                });

                if (output) {
                    // Group has sent some data
                    g.append('path')
                                .attr('d', d3.svg.symbol().type('triangle-up'))
                                .attr('fill', '#00ff00')
                                .attr('transform', `translate(0,${input?-6:3})`);
                }

                if (input) {
                    g.append('path')
                                .attr('d', d3.svg.symbol().type('triangle-down'))
                                .attr('fill', '#ff0000')
                                .attr('transform', `translate(0,${output?6:0})`);
                }
            });
        });
    },
    drawChords(chords, innerRadius) {
        //grouping and appending the Chords
        const chordsSel = {};

        chordsSel.update = this.canvas.selectAll('.chord path').data(chords);

        chordsSel.enter = chordsSel.update.enter();

        chordsSel.enter.append('g')
            .classed('chord', true)
            .append('path')
                .attr('d', d3.svg.chord().radius(innerRadius))
                .style('fill', d => {
                    return d.source.value>d.target.value ? colorScale(d.source.index) : colorScale(d.target.index);
                });
    },
    drag() {
        const e = $(ReactDOM.findDOMNode(this));

        const width = e.width();
        const height = e.height();

        const x1 = width / 2;
        const y1 = height / 2;
        const x2 = d3.event.x;
        const y2 = d3.event.y;

        const newAngle = Math.atan2(y2 - y1, x2 - x1) / (Math.PI / 180);

        this.canvas.attr('transform', `translate(${x1},${y1}) rotate(${newAngle},0,0)`);
    },
    // Returns an event handler for fading a given chord group.
    fade(opacity, i) {
        this.canvas.selectAll(".chord path")
                                .filter((d) => d.source.index != i && d.target.index != i)
                                .transition()
                                .style("opacity", opacity);
    },
    _onChange() {
        const storeData = ChordsDiagramStore.getData();
        const state = {loading: storeData.loading};

        if (!storeData.loading && !storeData.error) {
            const mpr = chordMpr(storeData.data);

            mpr.addValuesToMap('srcip')
                .addValuesToMap('dstip')
                .setFilter(function (row, a, b) {
                    return (row.srcip === a.name && row.dstip === b.name)
                })
                .setAccessor(function (recs, a, b) {
                    return recs.reduce((total, rec) => {
                        return total + (+rec.avgbyte);
                    }, 0);
                });

            const matrix = mpr.getMatrix();
            const map = mpr.getMap();

            const ipMap = Object.keys(map).sort((ip1, ip2) => map[ip1].index-map[ip2].index);

            state.data = {
                matrix,
                map: ipMap,
                rdr: chordRdr(matrix, map),
                ip: ChordsDiagramStore.getIp()
            };
        }

        this.replaceState(state);
    }
});

module.exports = DetailsChordsPanel;
