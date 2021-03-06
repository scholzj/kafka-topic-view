'use strict';

//import React from 'react';
//import ReactDOM from 'react';

class Partition extends React.Component   {
    constructor(props) {
        super(props);
    }

    render()    {
        const leader = this.props.partition.leader ? "leader" : "follower";
        const tooltip = `${this.props.partition.topic}-${this.props.partition.partition} ${this.props.partition.state} ${leader}`;
        const state = this.props.partition.state

        let stateClassName
        switch (state)  {
            case "online":
            case "in-sync":
                stateClassName = "bg-green"
                break;
            case "at-min-isr":
                stateClassName = "bg-yellow"
                break;
            case "under-min-isr":
                stateClassName = "bg-orange"
                break;
            case "offline":
                stateClassName = "bg-dark-red"
                break;
            default:
                stateClassName = "bg-grey"
        }

        stateClassName = "ma1 w1 h1 " + stateClassName

        return(
            <li className="relative">
                <div className={stateClassName} data-tooltip={tooltip} />
            </li>
        );
    }
}

class Broker extends React.Component   {
    constructor(props) {
        super(props);
    }

    render()    {
        return(
            <li className="w5 h-100 ma3">
                <div className="h-100">
                    <div className="flex items-end bg-dark-grey ba bw2 b--mid-grey w5 h-100 center pa1">
                        <ul className="list pl0 flex flex-wrap-reverse">
                            {this.props.partitions.map((partition) => (
                                <Partition partition={partition} key={partition.topic + "-" + partition.partition} />
                            ))}
                        </ul>
                    </div>
                    <p className="ttu tc b f5 lh-copy">Broker {this.props.brokerId}</p>
                </div>
            </li>
        );
    }
}

class KafkaTopicView extends React.Component {
    constructor(props) {
        super(props);

        this.state = { brokers: {} };
    }

    componentDidMount() {
        this.getTopicData()

        this.interval = setInterval(() => {
            this.getTopicData();
        }, 60000);
    }

    componentWillUnmount() {
        clearInterval(this.interval);
    }

    getTopicData()  {
        fetch('./api/topics')
            .then(res => res.json())
            .then((data) => {
                this.setState({ brokers: data.brokers })
            })
            .catch((error) => {
                this.setState({ brokers: {} })
                console.log(error)
            })
    }

    render() {
        return (
            <ul className="list pl0 flex flex-wrap center">
                {Object.keys(this.state.brokers).map((key) => (
                    <Broker key={key} brokerId={key} partitions={this.state.brokers[key].partitions} />
                ))}
            </ul>
        );
    }
}

ReactDOM.render(
    <KafkaTopicView />,
    document.getElementById('content')
)