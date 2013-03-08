%max nodes in dendrogram
nodes = 10;

loadtermdocdata;

% trim the tails (remove really frequent words, very rare words, and reps 
% that don't have more than 3000 words)
[id_wc_trim, id_words_trim, id_docs_trim] = trim(id_wc, id_words, id_docs, 3, 500, 30);
%clear id_wc % else we run out of memory in the next command

% run the LLR algorithm and remove any NaN and Inf vals
id_llr = llrmat(id_wc_trim);
id_fin = finitize(id_llr);

% generate the top 25 words for each doc
doc_top25words = topwordsall(id_fin, id_docs_trim{1}, id_words_trim, 25);

% create the linkage and the dendrogram
doc_tree = linkage(id_fin','complete','cosine');
[H,T] = dendrogram(doc_tree, nodes, 'labels', id_docs_trim{1},'orientation','right');
[H,T] = dendrogram(linkage(id_fin(:,1:50)','complete','cosine'),0, 'labels', id_docs_trim{1}(1:50),'orientation','right');

% store info about each of the nodes in the dendrogram
for j=1:nodes
    doc_node_info{j} = id_docs_trim{1}(find(T==j));
    for i=2:4
        doc_node_info{j} = [doc_node_info{j} id_docs_trim{i}(find(T==j))];
    end
end

% generate some stats to illustrate the content of each node
id_stats = nodestats(doc_node_info);

% latent semantic analysis to compute pairwise term similarity
[U,S,V] = svd(id_fin);
D=pdist(U*S,'cosine'); % compute distances between term vectors in the lower dimensional space
SquareD = squareform(D); % make it easier to dereference individual comparisons