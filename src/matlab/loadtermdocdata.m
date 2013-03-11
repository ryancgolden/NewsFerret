%----------------------------------------
% Load data for identity news word counts
%----------------------------------------

datDir = 'dat/';
tdDir = [datDir 'termdoc/'];

%-------------------------
% Identity News Docs
%-------------------------

% cell array of doc info
% title url author feedUri
fid = fopen([tdDir 'docs.dat']);
id_docs = textscan(fid,'%q %q %q %q');
fclose(fid);

% word count matrix for doc data
fid = fopen([tdDir 'wc.dat']);
% scanFormat is a long string of %f's for use by textscan
docCount = size(id_docs{1,1},1);
scanFormat = '%f';
for i=[1:docCount-1]
    scanFormat = [scanFormat ' %f'];
end
temp = textscan(fid,scanFormat);
id_wc = cell2mat(temp);
fclose(fid);

% array of all words used across docs
fid = fopen([tdDir 'words.dat']);
id_words = textscan(fid,'%q');
id_words = id_words{1};
fclose(fid);

% array of stop words
fid = fopen([datDir 'stopwords.dat']);
stopwords = textscan(fid,'%s');
stopwords = stopwords{1};
fclose(fid);
