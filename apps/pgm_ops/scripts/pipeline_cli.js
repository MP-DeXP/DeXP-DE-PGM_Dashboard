import { DEFAULT_EXTRACT_LOOKBACK_DAYS, DEFAULT_ROLE_HISTORY_MODE } from '../app/config/constants.js';

function readValue(args, index, inlineValue) {
    if (inlineValue !== undefined) {
        return inlineValue;
    }

    const next = args[index + 1];
    return next && !next.startsWith('--') ? next : true;
}

export function parsePipelineCliArgs(args = process.argv.slice(2)) {
    const options = {
        asOfDate: '',
        lookbackDays: DEFAULT_EXTRACT_LOOKBACK_DAYS,
        mxChannelId: '',
        mxPlatform: '',
        sample: false,
        roleHistoryMode: DEFAULT_ROLE_HISTORY_MODE,
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
                options.lookbackDays = value === true ? DEFAULT_EXTRACT_LOOKBACK_DAYS : value;
                break;
            case '--mx-channel-id':
                options.mxChannelId = value === true ? '' : String(value);
                break;
            case '--mx-platform':
                options.mxPlatform = value === true ? '' : String(value);
                break;
            case '--sample':
                options.sample = value;
                break;
            case '--role-history-mode':
                options.roleHistoryMode = value === true ? DEFAULT_ROLE_HISTORY_MODE : String(value);
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
