% Takes a word x doc matrix containing word counts for each doc
% and an array of words and removes the "tail" rows, i.e., words
% that appear fewer than min or more than max.  
% Returns [trimmed matrix, trimmed word array]
function [trimmat, trimwords, trimpeople] = trim(mat, words, docs, mincutoff, maxcutoff, mindocwords)
    wc = sum(mat')';
    rc = sum(mat)';
    % create proportions based on number of docs
    % assuming average doc words ~300
    numdocs = size(mat, 2);
    minword = numdocs * mincutoff * 300;
    maxword = numdocs * maxcutoff * 300;
    keepwords = wc < maxword & wc > minword;
    keepdocs = rc > mindocwords;
    trimmat = mat(keepwords,keepdocs);
    trimwords = words(keepwords);
    for i = 1:4
        trimpeople{i} = docs{i}(keepdocs);
    end