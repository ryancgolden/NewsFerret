% Main driver for identity theft news story analysis.
% Reads in term-doc matrix information, performs analysis and writes
% results
%
% Author: Ryan Golden
% Date: Spring 2013

%max nodes in dendrogram
nodes = 10;

baseLocationWin = 'C:\UT\github\ID\src\matlab\dat\';
baseLocationOther = '/win/UT/github/ID/src/matlab/dat/';
if ispc % true for windows
    baseLocation = baseLocationWin;
else
    baseLocation = baseLocationOther;
end
datDir = baseLocation;

% Load from dat files (produced by NewsFerret Java code)
[id_wc, id_words, id_docs, stopwords, important] = loadtermdocdata(fullfile(datDir, 'termdoc'), datDir, 4);

% remove stop words
[id_wc_stopped, id_words_stopped] = rmstopwords(id_wc, id_words, stopwords);

% trim the tails (remove really frequent words >1% of words, really rare words < .01%, and docs 
% that don't have more than 20 words)
[id_wc_trim, id_words_trim, id_docs_trim] = trim(id_wc_stopped, id_words_stopped, id_docs, 0.00005, 0.02, 20);
%[id_wc_trim, id_words_trim, id_docs_trim] = trim(id_wc, id_words, id_docs, 5, 600, 20);
%clear id_wc % else we run out of memory in the next command

% run the LLR algorithm and remove any NaN and Inf vals
id_llr = llrmat(id_wc_trim);
id_fin = finitize(id_llr);

% generate the top 25 words for each doc
doc_top25words = topwordsall(id_fin, id_docs_trim{1}, id_words_trim, 25);
doc_top25words = doc_top25words';
[nrows,ncols]= size(doc_top25words);
filename = fullfile(datDir, 'top25perdoc.csv');
fid = fopen(filename, 'w');
for row=1:nrows
    fprintf(fid, '\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n', doc_top25words{row,:});
end
fclose(fid);

% create the linkage and the dendrogram
%doc_tree = linkage(id_fin','complete','cosine');
%[H,T] = dendrogram(doc_tree, nodes, 'labels', id_docs_trim{1},'orientation','right');
%[H,T] = dendrogram(linkage(id_fin(:,1:50)','complete','cosine'),0, 'labels', id_docs_trim{1}(1:50),'orientation','right');

% store info about each of the nodes in the dendrogram
%for j=1:nodes
%    doc_node_info{j} = id_docs_trim{1}(find(T==j));
%    for i=2:4
%        doc_node_info{j} = [doc_node_info{j} id_docs_trim{i}(find(T==j))];
%    end
%end

% generate some stats to illustrate the content of each node
%id_stats = nodestats(doc_node_info);

% latent semantic analysis to compute pairwise term similarity
% first compute singular value decomposition
[U,S,V] = svd(id_fin);
% now reduce the rank.  50 < k < 1000 has been shown to work well,
% with 100 typically performing well, though no theory exists to explain why
k = 100;
Uk = U(:,1:k);
Sk = S(1:k,1:k);
Vk = V(:,1:k);

D=pdist(Uk*Sk,'cosine'); % compute distances between term vectors in the lower dimensional space
SquareD = squareform(D); % make it easier to dereference individual comparisons
[pair_sims, pair_related_docs] = termsim(SquareD, id_words_trim, important, 1, Uk, Sk, Vk, id_docs_trim{1}, 5);
%pair_sims = sortcell(pair_sims,1);

% Write important pair weights to a file

% 1-cosine sim gives us a positive weight that increases with similarity
% for our visualization tools
for row=1:size(pair_sims,1)
    if row==1
        tmp = {pair_sims{row,1:2} 1.0000-cell2mat(pair_sims(row,3))};
    else
        tmp = [tmp; pair_sims(row,1:2) {1.0000-cell2mat(pair_sims(row,3))}];
    end
end
pair_sims = tmp;

[nrows,ncols]= size(pair_sims);
filename = fullfile(datDir, 'importantpairs.csv');
fid = fopen(filename, 'w');
fprintf(fid, '\"%s\" \"%s\" \"%s\" \"%s\"\n', 'source','target','weight','type');
for row=1:nrows
    fprintf(fid, '%s %s %1.4f undirected\n', pair_sims{row,:});
end
fclose(fid);

[nrows,ncols]= size(pair_related_docs);
filename = fullfile(datDir, 'importantpairs_topmatchingdocs.csv');
fid = fopen(filename, 'w');
fprintf(fid, '\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n', 'source','target','weight','type','rank1','rank2','rank3','rank4','rank5');
for row=1:nrows
    fprintf(fid, '\"%s\",\"%s\",\"%1.4f\",\"undirected\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n', pair_related_docs{row,:});
end
fclose(fid);

