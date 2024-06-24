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

	const { id } = await Actor.call(actorName, { testOut: true, ...actorInput });
	runId = id;

	log.info("Actor finished..")
}

const run = client.run(runId);

const keyValue = run.keyValueStore()

const schemas = ((await keyValue.getRecord('IO_SCHEMAS'))?.value as Record<string, any>) || {}
const outputData = await run.dataset().listItems() as { items: any[] }

const compiledOutputSchemas: Record<string, any> = []

for (const [schemaName, schema] of Object.entries(schemas['OUTPUT']) || {}) {
	compiledOutputSchemas[schemaName] = parseAsSchema(schema)
}

for (const entry of outputData.items) {
	let passed = false
	const errors: Record<string, type.errors> = {};

	for (const [schemaName, schema] of Object.entries(compiledOutputSchemas)) {
		const result = schema(entry)

		if (result instanceof type.errors) {
			errors[schemaName] = result;
		}

		passed = true;
		break;
	}

	if (!passed) {
        for (const [schemaName, error] of Object.entries(errors)) {
            log.error(`Error with schema: ${schemaName}\n${error!.summary}`);
        }
    }
}

await Actor.exit();
