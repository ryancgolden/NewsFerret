function [wc_stopped, words_stopped] = rmstopwords(wc, words, stopwords)
%RMSTOPWORDS Remove stop words from term-doc and term arrays
% Args:
%   wc - term x doc matrix
%   words - corresponding terms (same index as wc term dimension)
%   stopwords - 1xN array of words to remove
% Output:
%   [wc_stopped, words_stopped]
%-------------------------------------------------------

wc_stopped = wc;
words_stopped = words;

% create an index to the words that aren't stopped
notstopidx = ~ismember(words, stopwords);

% returns only unstopped words
words_stopped = words(notstopidx);

% returns only rows for unstopped words
wc_stopped = wc(notstopidx, :);

end

