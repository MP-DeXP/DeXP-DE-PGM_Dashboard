export function buildBhiContextStub(latestDate) {
    return {
        date: latestDate,
        bhi_available: 'false',
        bhi_status: 'deferred',
        bhi_note: 'BHI canonical source is explicitly out of scope for pgm_ops v0.'
    };
}
