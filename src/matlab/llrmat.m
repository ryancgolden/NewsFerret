%--------------------------------------------------------------
% Take a word x document matrix containing word counts
% and return a matrix of the same
% size with log-likelihood ratio values.  LLR will be between
% each document (column) and all the other documents.
%--------------------------------------------------------------
function llr_matrix = llrmat(data)
[numWords, numReps] = size(data);
all_words_all_reps = sum(sum(data));
llr_matrix = zeros(numWords,numReps);  %pre-allocate

for currentRep=1:numReps,
    all_words_this_rep = sum(data(:,currentRep));
    all_words_other_reps = all_words_all_reps - all_words_this_rep;

    n1 = all_words_this_rep;
    n2 = all_words_other_reps;

    for currentWord=1:numWords,
        this_word_all_reps = sum(data(currentWord,:));
        this_word_this_rep = data(currentWord,currentRep);
        this_word_other_reps = this_word_all_reps - this_word_this_rep;

        k1 = this_word_this_rep;
        k2 = this_word_other_reps;
        
        p1 = k1/n1;
        p2 = k2/n2;
        p = (k1+k2)/(n1+n2);         
        a = (p1/p)^k1;
        b = ((1-p1)/(1-p))^(n1-k1);
        c = (p2/p)^k2;
        d = ((1-p2)/(1-p))^(n2-k2);
        tmp = a*b*c*d;
       
        tmp = sign(p1-p2)*2*log(tmp);
        llr_matrix(currentWord,currentRep) = tmp;
    end
end



%p1 = k1/n1;
%p2 = k2/n2;
%p = (k1+k2)/(n1+n2);
%a = (p1/p)^k1;
%b = ((1-p1)/(1-p))^(n1-k1);
%c = (p2/p)^k2;
%d = ((1-p2)/(1-p))^(n2-k2);
%result = 2*log(a*b*c*d)
