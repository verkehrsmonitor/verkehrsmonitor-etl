function* range(begin, end) {
    // `end + 1` because the generator will skip the last value
    for (let i = begin; i < end + 1; i += 1) {
        yield i;
    }
};

module.exports = {
    range
};
