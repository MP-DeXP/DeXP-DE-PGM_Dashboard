import { main as runExtract } from './run_extract.js';
import { main as runTransform } from './run_transform.js';
import { main as runViewModels } from './run_view_models.js';
import { main as runValidations } from './run_validations.js';

await runExtract();
await runTransform();
await runViewModels();
await runValidations();
