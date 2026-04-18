export const APP_ROOT_URL = new URL('../../', import.meta.url);

export const ARTIFACT_DIR_URLS = {
    raw_extract: new URL('../../artifacts/raw_extract/', import.meta.url),
    staging: new URL('../../artifacts/staging/', import.meta.url),
    mart: new URL('../../artifacts/mart/', import.meta.url),
    view_model: new URL('../../artifacts/view_model/', import.meta.url),
    qa: new URL('../../artifacts/qa/', import.meta.url)
};

export function getBrowserArtifactPath(layer, filename, baseOverride = '/api/pgm-ops') {
    const normalizedBase = baseOverride.replace(/\/$/, '');
    const apiLayerMap = {
        view_model: 'view-model'
    };
    const layerSegment = normalizedBase.endsWith('/api/pgm-ops')
        ? (apiLayerMap[layer] ?? layer)
        : layer;

    return `${normalizedBase}/${layerSegment}/${filename}`;
}
