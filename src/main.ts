import { Actor, log } from 'apify';
import { scope } from 'arktype';
await Actor.init();

interface Input {
	actorName: string,
	runId?: string
}

let {
	actorName,
	runId
} = (await Actor.getInput<Input>())!

const client = Actor.newClient();

if (!runId) {
	log.info("Starting the Actor..")

	const { id } = await Actor.call(actorName, { testIO: true });
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
	compiledInputSchemas[label] = scope(schemas['INPUT'][label]).compile()
}

for (const schema of schemas['OUTPUT']) {
	compiledOutputSchemas.push(scope(schema).compile())
}

for (const label of Object.keys(inputData)) {
	for (const entry of inputData[label]) {
		const { data, problems } = compiledInputSchemas[label].$type(entry);

		if (problems) {
			log.error(problems)	
		}
	}
}

for (const entry of outputData.items) {
	let valid = false

	for (const outputSchema of compiledOutputSchemas) {
		const { data, problems } = outputSchema.$type(entry);

		if (!problems) valid = true;
	}

	if (!valid) {
		log.error(`Output error on entry: ${JSON.stringify(entry, null, 4)}`)
	}
}

await Actor.exit();
