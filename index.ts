import {io, iot, mqtt} from "aws-iot-device-sdk-v2";
import {IotJobsClient} from "aws-iot-device-sdk-v2/dist/iotjobs/iotjobsclient";
import {JobStatus, UpdateJobExecutionRequest} from "aws-iot-device-sdk-v2/dist/iotjobs/model";
import Timeout = NodeJS.Timeout;

type Args = { [index: string]: any };

var timeout: Timeout;
var interval: Timeout;

const yargs = require('yargs');
yargs.command('*', false, (yargs: any) => {
    yargs
        .option('endpoint', {
            description: "Your AWS IoT custom endpoint, not including a port. "  +
                "Ex: \"abcd123456wxyz-ats.iot.us-east-1.amazonaws.com\"",
            type: 'string',
            required: false
        }).option('client_cert', {
            description: "Your client certificate",
            type: 'string',
            required: true
        }).option('client_key', {
            description: "Your client certificate key",
            type: 'string',
            required: true
        }).option('ca_cert', {
            description: "Your CA certificate",
            type: 'string',
            required: true
        }).option('verbosity', {
            alias: 'v',
            description: 'BOOLEAN: Verbose output',
            type: 'string',
            default: 'info',
            choices: ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'none']
        }).option('topic', {
            description: 'the topic to publish and subscribe to',
            type: 'string',
            default: 'test-topic'
        }).option('client_id', {
            description: 'the topic to publish and subscribe to',
            type: 'string',
            default: 'aukes-device'
        })
        .help()
    .alias('help', 'h')
    .showHelpOnFail(false);
}, main).parse();

function requestNewJob(jobsClient: IotJobsClient, request: { thingName: any }) {
    console.log("Informing IoT Core that we're ready for another job")
    jobsClient.publishStartNextPendingJobExecution(request, 0).then(value => {
        console.log('value: ' + JSON.stringify(value));
    });
}

async function subscribeToJobs(connection: mqtt.MqttClientConnection, argv: Args) {

    let jobsClient = new IotJobsClient(connection);

    let request = {
        'thingName': argv.client_id
    };

    interval = setInterval(() => {
            requestNewJob(jobsClient, request);
            }, 10000)

    await jobsClient.subscribeToStartNextPendingJobExecutionAccepted(request, 0, (error, response) => {
        console.log("subscribeToStartNextPendingJobExecutionAccepted, response: ", error, response);

        console.log('########################');
        console.log('Job execution received: ');
        console.log(JSON.stringify(response));
        console.log('########################');

        let jobExecutionAcceptedRequest: UpdateJobExecutionRequest = {
            'jobId' : response?.execution?.jobId || "unknown",
            'thingName' : argv.client_id,
            'status' : JobStatus.SUCCEEDED
        };

        console.log("subscribeToStartNextPendingJobExecutionAccepted - sending accepted request", JSON.stringify(jobExecutionAcceptedRequest));
        jobsClient.publishUpdateJobExecution(jobExecutionAcceptedRequest, 0);

    }).then(r => {
        console.log('subscribeToStartNextPendingJobExecutionAccepted - completion', JSON.stringify(r));
    });

    await jobsClient.subscribeToGetPendingJobExecutionsAccepted(request, 0, (error, response) => {
        console.log("subscribeToGetPendingJobExecutionsAccepted, response: ", error, response);
    }).then(r => {
        console.log('subscribeToGetPendingJobExecutionsAccepted - completion: ' + JSON.stringify(r));
    });
}

function startTimer() {
    timeout = setTimeout(args => {
        restartTimer(timeout);
    }, 30000);
}

function restartTimer(timeout: NodeJS.Timeout) {
    if (timeout){
        clearTimeout(timeout);
    }
    startTimer();
}

async function main(argv: Args) {
    if (argv.verbosity != 'none') {
        const level : io.LogLevel = parseInt(io.LogLevel[argv.verbosity.toUpperCase()]);
        io.enable_logging(level);
    }

    const client_bootstrap = new io.ClientBootstrap();

    let config_builder = iot.AwsIotMqttConnectionConfigBuilder.new_mtls_builder_from_path(argv.client_cert, argv.client_key);

    config_builder.with_certificate_authority_from_path(undefined, argv.ca_cert);

    config_builder.with_clean_session(false);
    console.log(`clientId: ${argv.client_id}`);
    config_builder.with_client_id(argv.client_id || "test-" + Math.floor(Math.random() * 100000000));
    config_builder.with_endpoint(argv.endpoint);

    startTimer();

    const config = config_builder.build();
    const client = new mqtt.MqttClient(client_bootstrap);
    const connection = client.new_connection(config);

    await connection.connect().then(res => {
        console.log(`Connected? ${res}`);
    })

    console.log('connected');
    subscribeToJobs(connection, argv);

}

process.on("SIGTERM", signal => {
    if (timeout) {
        clearTimeout(timeout);
        clearInterval(interval);
    }
});

process.on('SIGINT', signal => {
    if (timeout) {
        clearTimeout(timeout)
        clearInterval(interval);
    }
});
