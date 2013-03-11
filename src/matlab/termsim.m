%------------------------------------------------------
% Create pairwise term similarity cell array
% Input:
%   array - symmetric similarity matrix, e.g., squareform(pdist(...))
%   terms - Cell array of indexed terms, matching sim matrix cols and rows
%   threshold - a value between 0 and 1 that will be the cutoff
%      for which pairs will be 
% Output:
%   An M x 3 cell array of pairwise term similarity
%   where M is the number of pairs and each row has:
%       'term1' 'term2' [similarity_measure]
%------------------------------------------------------
function c = termsim(array, terms, threshold)
s = size(array);
numelems = numel(array);
% This for knowing MxN index; facilitates term retrieval
[i,j] = ind2sub(s,1:numelems);

% iterate through the area above the diagonal in the sim matrix
% looking up the term names based on location
% and populate our cell array
c = cell(1,3); % initialize
c{1} = ['dummy1' 'dummy2', 0.5];
for idx = 1:numelems
    if (i(idx) >= j(idx)) % this keeps us above the diagonal
        continue;
    end
    element = array(idx);
    if element > 0 && element <= threshold
        %disp(['element at ', num2str(i(idx)), ',', num2str(j(idx)), ' is ', num2str(element)]);
        %disp([id_words_trim{i(idx)}, ' and ', id_words_trim{j(idx)}, ' = ', num2str(element)]);
        
        % append pair to our resulting 3-wide cell array
        newrow = { terms{i(idx)}, terms{j(idx)}, element };
        c = [c ; newrow];
    end
end
tmp = c(2:end,:); % remove dummy initial row
c = tmp;
    