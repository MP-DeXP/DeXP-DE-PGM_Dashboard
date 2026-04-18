export const APP_SETTINGS = {
    brandName: 'Mercury Skin',
    appTitle: '브랜드 운영 상태판',
    localStorageArtifactKey: 'pgm_ops_artifact_base',
    defaultArtifactBase: '/api/pgm-ops',
    validationFreshnessGraceDays: 3650,
    supportedQueryParams: ['artifactBase', 'sample'],
    sampleModeReason: '브라우저에서 /api/pgm-ops/view-model CSV를 읽지 못할 때만 내장 sample fallback을 사용합니다. 비어 있는 artifact는 sample로 대체하지 않습니다.'
};
