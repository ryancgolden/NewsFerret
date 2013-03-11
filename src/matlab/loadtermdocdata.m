function [wc, words, docs, stops, important] = loadtermdocdata(tdDir, datDir, docDims)
% LOADTERMDOCDATA Loads term-document data from NewsFerret dat files
%-------------------------------------------------------------------
% Load data from:
%   wc.dat: word count (term(N) x doc(M))
%   words.dat: (1xN)
%   docs.dat: MXN, where M is determined by
%   importantwords.dat (1XN) of important terms
% number of metadata fields about each doc
%   stopwords.dat: (1XN) dat files
% Output:
%   [wc, words, docs, stops]
% Args:
%   tdDir - dir containing wc, words, and docs dat files
%   datDir - dir containint stopwords.dat file
%   docDims - number of metadata fields in docs.dat
%------------------------------------------------------------------

    % cell array of doc info
    % title url author feedUri
    fid = fopen([tdDir 'docs.dat']);
    docScanFormat = '%q';
    for i=[1:docDims-1]
        docScanFormat = [docScanFormat ' %q'];
    end
    docs = textscan(fid, docScanFormat);
    fclose(fid);

    % word count matrix for doc data
    fid = fopen([tdDir 'wc.dat']);
    % scanFormat is a long string of %f's for use by textscan
    docCount = size(docs{1,1},1);
    scanFormat = '%f';
    for i=[1:docCount-1]
        scanFormat = [scanFormat ' %f'];
    end
    temp = textscan(fid,scanFormat);
    wc = cell2mat(temp);
    fclose(fid);

    % array of all words used across docs
    fid = fopen([tdDir 'words.dat']);
    words = textscan(fid,'%q');
    words = words{1};
    fclose(fid);

    % array of stop words
    fid = fopen([datDir 'stopwords.dat']);
    stops = textscan(fid,'%s');
    stops = stops{1};
    fclose(fid);

    % array of important words
    fid = fopen([datDir 'importantwords.dat']);
    important = textscan(fid,'%s');
    important = important{1};
    fclose(fid);
    
end
