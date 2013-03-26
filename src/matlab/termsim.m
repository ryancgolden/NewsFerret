%------------------------------------------------------
% Create pairwise term similarity cell array for important
% pairs
%
% Input:
%   array - symmetric similarity matrix, e.g., squareform(pdist(...))
%   terms - Cell array of indexed terms, matching sim matrix cols and rows
%   importantterms - Cell array of important terms.  This will be used
%       to reduce the input space.  Processing time is O(importantterms^2)
%   threshold - a value between 0 and 1 that will be the cutoff
%      for which pairs will be allowed through
%   pUk, pSk, pVk - reduced rank SVD of term-doc matrix
%   pDocs - doc name indexed to Vk
%   pNumRelatedDocs - number of related docs to name for each pair in the
%       pair_related_docs cell-array that is returned.
% Output:
%   An M x 3 cell array of pairwise term similarity
%   where M is the number of pairs and each row has:
%       'term1' 'term2' [similarity_measure]
%------------------------------------------------------
function [pair_sims, pair_related_docs] = termsim(pArray, pTerms, pImportant, pThreshold, pUk, pSk, pVk, pDocs, pNumRelatedDocs)

importantidx = ismember(pTerms, pImportant);
% Only let important word pairs through
array = pArray(importantidx, importantidx);
terms = pTerms(importantidx);

s = size(array);
numelems = numel(array);
% This for knowing MxN index; facilitates term retrieval
[i,j] = ind2sub(s,1:numelems);

% iterate through the area above the diagonal in the sim matrix
% looking up the term names based on location
% and populate our cell array
pair_sims = cell(1,3); % initialize
pair_sims{1} = ['dummy1' 'dummy2', 0.5];
pair_related_docs = cell(1, (3 + pNumRelatedDocs));
for idx = 1:numelems
    if (i(idx) >= j(idx)) % this keeps us above the diagonal
        continue;
    end
    element = array(idx);
    if element > 0 && element <= pThreshold
        %disp(['element at ', num2str(i(idx)), ',', num2str(j(idx)), ' is ', num2str(element)]);
        %disp([id_words_trim{i(idx)}, ' and ', id_words_trim{j(idx)}, ' = ', num2str(element)]);
        
        term1 = terms{i(idx)};
        term2 = terms{j(idx)};
        
        % append pair to our resulting 3-wide cell array
        newrow = {term1, term2, element};
        pair_sims = [pair_sims ; newrow];
        %idxStr = num2str(idx);
        %disp(['added row for idx ' idxStr]);
        
        % append related docs by LSI similarity rank
        % XXX: not using pNumRelatedDocs yet... lazy
        % XXX: this is very slow... is there a faster way to do this?
        r = query(pUk, pSk, pVk, pTerms, pDocs, {term1 term2});
        newrelatedrow = {term1, term2, element, r{1,1}, r{2,1}, r{3,1}, r{4,1}, r{5,1}};
        %disp(newrelatedrow);
        pair_related_docs = [pair_related_docs; newrelatedrow];
    end
end

tmp = pair_sims(2:end,:); % remove dummy initial row
pair_sims = sortcell(tmp,1); % return sorted on first column

tmp = pair_related_docs(2:end,:); % remove dummy initial row
pair_related_docs = sortcell(tmp,1); % return sorted on first column

