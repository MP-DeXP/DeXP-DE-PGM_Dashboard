import { DEFAULT_AS_OF_DATE, DEFAULT_LOOKBACK_DAYS } from '../app/config/constants.js';

function readValue(args, index, inlineValue) {
    if (inlineValue !== undefined) {
        return inlineValue;
    }

    const next = args[index + 1];
    return next && !next.startsWith('--') ? next : true;
}

export function parsePipelineCliArgs(args = process.argv.slice(2)) {
    const options = {
        asOfDate: DEFAULT_AS_OF_DATE,
        lookbackDays: DEFAULT_LOOKBACK_DAYS,
        refreshRosetta: false
    };

    for (let index = 0; index < args.length; index += 1) {
        const argument = args[index];
        if (!argument.startsWith('--')) {
            continue;
        }

        const [flag, inlineValue] = argument.split('=');
        const value = readValue(args, index, inlineValue);

        switch (flag) {
            case '--as-of-date':
                options.asOfDate = value === true ? '' : String(value);
                break;
            case '--lookback-days':
                options.lookbackDays = value === true ? DEFAULT_LOOKBACK_DAYS : Number.parseInt(value, 10) || DEFAULT_LOOKBACK_DAYS;
                break;
            case '--refresh-rosetta':
                options.refreshRosetta = value === true ? true : String(value) !== 'false';
                break;
            default:
                break;
        }

        if (inlineValue === undefined && value !== true) {
            index += 1;
        }
    }

    return options;
}
