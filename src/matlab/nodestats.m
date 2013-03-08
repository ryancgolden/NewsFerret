% Create a 2D cell array with information about the 
% contents of each node in the node_info object passed in
function final = nodestats(node_info)
    nodes = size(node_info,2);
    %attnum = size(node_info{1},2);
    final = {'Node' 'Stat'};
    for i = 1:nodes
        node_stats{i} = {['Node' num2str(i)] 'Count'};
        for att = [2 3 4] %attnum... sorry, hard-coding
            vals = node_info{i}(:,att);
            unqvals = unique(vals);
            numunique = size(unqvals,1);
            for j = 1:numunique
                count = size(find(ismember(vals,unqvals(j))==1),1);
                node_stats{i} = [node_stats{i} ; unqvals(j) num2cell(count)];
            end
        end
        final = [final ; {' ' ' '} ; node_stats{i}];
    end