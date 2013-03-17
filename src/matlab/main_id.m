%max nodes in dendrogram
nodes = 10;

datDir = '/win/UT/github/ID/src/matlab/dat';

% Load from dat files (produced by NewsFerret Java code)
[id_wc, id_words, id_docs, stopwords, important] = loadtermdocdata([datDir '/termdoc/'], [datDir '/'], 4);

%remove stop words
[id_wc_stopped, id_words_stopped] = rmstopwords(id_wc, id_words, stopwords);

% trim the tails (remove really frequent words >1% of words, really rare words < .01%, and docs 
% that don't have more than 20 words)
[id_wc_trim, id_words_trim, id_docs_trim] = trim(id_wc_stopped, id_words_stopped, id_docs, 0.0001, 0.01, 20);
%[id_wc_trim, id_words_trim, id_docs_trim] = trim(id_wc, id_words, id_docs, 5, 600, 20);
%clear id_wc % else we run out of memory in the next command

% run the LLR algorithm and remove any NaN and Inf vals
id_llr = llrmat(id_wc_trim);
id_fin = finitize(id_llr);

% generate the top 25 words for each doc
doc_top25words = topwordsall(id_fin, id_docs_trim{1}, id_words_trim, 25);
doc_top25words = doc_top25words';
[nrows,ncols]= size(doc_top25words);
filename = [datDir '/top25perdoc.csv'];
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
[U,S,V] = svd(id_fin);
D=pdist(U*S,'cosine'); % compute distances between term vectors in the lower dimensional space
SquareD = squareform(D); % make it easier to dereference individual comparisons
pair_sims = termsim(SquareD, id_words_trim, 1);
pair_sims = sortcell(pair_sims,1);

% Only let important word pairs through
important_pairs = pair_sims(ismember(pair_sims(:,1), important), :);
important_pairs = important_pairs(ismember(important_pairs(:,2), important), :);

% Write important pair weights to a file
[nrows,ncols]= size(important_pairs);
filename = [datDir '/importantpairs.dat'];
fid = fopen(filename, 'w');
for row=1:nrows
    fprintf(fid, '%s %s %1.4f \n', important_pairs{row,:});
    fprintf(fid, '%s %s %1.4f \n', important_pairs{row,[2 1 3]}); % print inverse also
end
fclose(fid);
