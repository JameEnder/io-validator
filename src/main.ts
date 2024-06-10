import { Actor, log } from 'apify';
import { type } from 'arktype';
import { parseAsSchema } from '@arktype/schema';
await Actor.init();

interface Input {
	actorName: string,
	runId?: string,
	actorInput?: Record<string, any>,
}

let {
	actorName,
	runId,
	actorInput = {}
} = (await Actor.getInput<Input>())!

const client = Actor.newClient();

if (!runId) {
	log.info("Starting the Actor..")

	const { id } = await Actor.call(actorName, { testIO: true, ...actorInput });
	runId = id;

	log.info("Actor finished..")
}

const run = client.run(runId);

const keyValue = run.keyValueStore()

const schemas = (await keyValue.getRecord('IO_SCHEMAS'))!.value as Record<string, any>
const inputData = (await keyValue.getRecord('IO_DATA'))!.value as Record<string, any[]>
const outputData = await run.dataset().listItems() as { items: any[] }


const compiledInputSchemas: Record<string, any> = {}
const compiledOutputSchemas: any[] = []

for (const label of Object.keys(schemas['INPUT'])) {
	compiledInputSchemas[label] = parseAsSchema(schemas['INPUT'][label])
}

for (const schema of schemas['OUTPUT']) {
	compiledOutputSchemas.push(parseAsSchema(schema))
}

for (const label of Object.keys(inputData)) {
	for (const entry of inputData[label]) {
		const out = compiledInputSchemas[label](entry);

		if (out instanceof type.errors) {
			log.error(out.summary)	
		}
	}
}

for (const entry of outputData.items) {
	let valid = false

	for (const outputSchema of compiledOutputSchemas) {
		const out = outputSchema(entry)

		if (!(out instanceof type.errors)) {
			valid = true;
			break;
		}
	}

	if (!valid) {
		log.error(`Output error on entry: ${JSON.stringify(entry, null, 4)}`)
	}
}

await Actor.exit();
