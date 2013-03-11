% Takes a word x doc matrix containing word counts for each doc
% and an array of words and removes the "tail" rows, i.e., words
% that appear fewer than min or more than max.  
% Returns [trimmed matrix, trimmed word array]
function [trimmat, trimwords, trimpeople] = trim(mat, words, docs, minword, maxword, mindocwords)
    wc = sum(mat')';
    rc = sum(mat)';
    keepwords = wc < maxword & wc > minword;
    keepdocs = rc > mindocwords;
    trimmat = mat(keepwords,keepdocs);
    trimwords = words(keepwords);
    for i = 1:4
        trimpeople{i} = docs{i}(keepdocs);
    end