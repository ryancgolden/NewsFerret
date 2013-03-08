%----------------------------------------
% Load data for identity news word counts
%----------------------------------------

%-------------------------
% Identity News Docs
%-------------------------
% word count matrix for doc data
fid = fopen('dat/wc.dat')
temp = textscan(fid,'%f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f %f');
id_wc = cell2mat(temp);
fclose(fid);

% array of all words used across docs
fid = fopen('dat/words.dat');
id_words = textscan(fid,'%q');
id_words = id_words{1};
fclose(fid);

% cell array of doc info
% title url author feedUri
fid = fopen('dat/docs.dat');
id_docs = textscan(fid,'%q %q %q %q');
fclose(fid);

% array of stop words
fid = fopen('dat/stopwords.dat');
stopwords = textscan(fid,'%s');
stopwords = stopwords{1};
fclose(fid);


