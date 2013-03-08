% takes an MxN matrix and returns an MxN matrix with all Inf and NaN
% values set to 0
function finitized = finitize(mat)
    finitized = mat;
    nanlocations = find(isnan(finitized));
    inflocations = find(isinf(finitized));
    finitized(nanlocations) = 0;
    finitized(inflocations) = 0;

