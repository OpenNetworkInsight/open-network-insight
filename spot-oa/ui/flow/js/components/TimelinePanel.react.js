const React = require('react') ;

const ChartMixin = require('../../../js/components/ChartMixin.react');
const ContentLoaderMixin = require('../../../js/components/ContentLoaderMixin.react');
const TimelineStore = require('../stores/TimelineStore');
const TimelineMixin = require('../../../js/components/TimelineMixin.react');

var TimelinePanel = React.createClass({
    mixins: [ContentLoaderMixin, ChartMixin, TimelineMixin],
    componentDidMount: function ()
    {
        TimelineStore.addChangeDataListener(this._onChange);
        window.addEventListener('resize', this.buildChart);
    },
    componentWillUnmount: function ()
    {
        TimelineStore.removeChangeDataListener(this._onChange);
        window.addEventListener('resize', this.buildChart);
    },
    _onChange() {
        const storeData = TimelineStore.getData();

        if (storeData.loading || storeData.error) {
            this.setState(storeData);
        }
        else {
            const state = this._getStateFromStoreData(storeData.data);

            this.setState(state);
        }
    },
    _getStateFromStoreData: function (data)
    {
        const state = {
            loading: false,
            name: TimelineStore.getFilterValue(),
            date: TimelineStore.getDate(),
            data: {}
        };

        /*
            Build a state similar to:

            {
                loading: false,
                name: 'IP_OF_INTEREST',
                date: 'CURRENT_DATE',
                data: {
                    'FIRST_UNIQUE_IP': {
                        name: 'FIRST_UNIQUE_IP',
                        data: {
                            'YYYY-MM-DD HH:MM': {
                                ip: 'FIRST_UNIQUE_IP',
                                date: 'YYYY-MM-DD HH:MM',
                                ports: {
                                    '80': 10,
                                    '443': 1
                                }
                            }
                        }
                    }
                }
            }

            And then pop-up every object value and replace 'data' fields with an
            array of object values. Turn objects into arrays
        */

        data.forEach(item => {
            [
                {ipField:'srcip', portField:'sport'},
                {ipField:'dstip', portField:'dport'}
            ].forEach(({ipField, portField}) => {
                var id, ip, port;

                ip = item[ipField];
                port = item[portField];

                if (ip==state.name) return;

                if (!state.data[ip]) {
                    state.data[ip] = {
                        name: ip,
                        data: {}
                    };
                }

                id = item.tstart.substr(0, 16);
                if (!state.data[ip].data[id]) {
                    state.data[ip].data[id] = {
                        name: ip,
                        date: id,
                        ports: {}
                    };
                }

                if (!state.data[ip].data[id].ports[port]) {
                    state.data[ip].data[id].ports[port]=0;
                }

                state.data[ip].data[id].ports[port]++;
            });
        });

        state.data = Object.keys(state.data).map(ip => {
            // Looking at ip data
            state.data[ip].data = Object.keys(state.data[ip].data).map((id) => {
                // Looking at date data

                // Find the most referenced port
                state.data[ip].data[id].port = Object.keys(state.data[ip].data[id].ports).reduce((currentPort, port) => {
                    if (!currentPort) return port;

                    return state.data[ip].data[id].ports[currentPort]>=state.data[ip].data[id].ports[port] ? currentPort: port;
                }, null);

                // we have found the most common ports, get rid of port data
                delete state.data[ip].data[id].ports;

                // Unwrap date data
                return state.data[ip].data[id];
            });

            // Unwrap ip data
            return state.data[ip];
        });

        return state;
    },
    getTooltipContent (e) {
        return `${e.name}: On ${e.date}, the most used port was ${e.port}`;
    }
});

module.exports = TimelinePanel;
