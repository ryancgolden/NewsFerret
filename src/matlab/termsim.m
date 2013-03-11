%------------------------------------------------------
% Create pairwise term similarity cell array
% Input:
%   Similarity matrix
%   Cell array of indexed terms, matching sim matrix
% Output:
%   An M x 3 cell array of pairwise term similarity
%   where M is the number of pairs and each row has:
%       'term1' 'term2' [similarity_measure]
%------------------------------------------------------
function c = term_sim(array, terms)
s = size(array);
numelems = numel(array);
% This for knowing MxN index; facilitates term retrieval
[i,j] = ind2sub(s,1:numelems);

c = cell(1,3); % initialize
for idx = 1:numelems
    element = array(idx);
    if element > 0 && element < 0.5
        %disp(['element at ', num2str(i(idx)), ',', num2str(j(idx)), ' is ', num2str(element)]);
        %disp([id_words_trim{i(idx)}, ' and ', id_words_trim{j(idx)}, ' = ', num2str(element)]);
        
        % append pair to our resulting 3-wide cell array
        newrow = { terms{i(idx)}, terms{j(idx)}, element };
        c = [c ; newrow];
    end
end
    