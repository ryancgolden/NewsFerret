function titles = query(Uk, Sk, Vk, terms, docs, queryterms)
%query Returns a list of the ranked matches for an LSI query
% for the given reduced rank U, S, and V (an SVD) and the 
% corresponding terms and docs labels and the query terms

%titles = {'some result' 'another result'};

queryvector = ismember(terms, queryterms);
red_q = queryvector' * Uk * Sk';


titles = cell(size(Vk,1),2); % initialize
%titles{1} = ['dummy1', 0.5];
for idx = 1:size(Vk,1)
        
    similarity = pdist([red_q; (Sk*(Vk(idx,:)'))'], 'cosine');
    titles{idx,1} = docs{idx};
    titles{idx,2} = similarity;

end
%tmp = titles(2:end,:); % remove dummy initial row
titles = sortcell(titles,2); % sort on second column
