% Return the top n words for each representative/senator in a multidimensional
% cell array
% 
% name - String name of rep
% llrmat - word x rep LLR-rankings matrix
% replist - cell array of representative/senator String names
% wordlist - cell array String words
% topnum - how many of the top words you want to see (e.g, 10, 50, etc.)
%-------------------------------------------------------------
function result = topwordsall(llrmat, replist, wordlist, topnum)
    
    [numwords, numreps] = size(llrmat);
    % init cell array, adding one row for the rep name
    result = cell(topnum+1,1);

    for repnum = 1:numreps
        name = replist{repnum};
        %repnum = find(ismember(replist, name));
        repcol = llrmat(:,repnum);
        [OldRowNumber, NewRowNumber] = sort(repcol);
        repwords = wordlist(NewRowNumber);
        represult = repwords(numwords-topnum+1:numwords);
        represult = represult(end:-1:1);
        %name
        represult = [name;represult];
        %represult
        result = [result,represult];
    end
    
    result = result(:,2:end);

