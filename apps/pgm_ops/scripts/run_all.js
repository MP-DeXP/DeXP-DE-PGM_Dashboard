import { main as runExtract } from './run_extract.js';
import { main as runTransform } from './run_transform.js';
import { main as runViewModels } from './run_view_models.js';
import { main as runValidations } from './run_validations.js';

const argv = process.argv.slice(2);

await runExtract(argv);
await runTransform(argv);
await runViewModels(argv);
await runValidations(argv);
