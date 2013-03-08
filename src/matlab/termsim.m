array = SquareD;
s = size(SquareD);
%for idx = 1:numel(array)
numelems = numel(array);
[i,j] = ind2sub(s,1:numelems);

c = cell(1,3);
for idx = 1:numelems
    element = array(idx);
    if element > 0 && element < 0.5
        %disp(['element at ', num2str(i(idx)), ',', num2str(j(idx)), ' is ', num2str(element)]);
        %disp([id_words_trim{i(idx)}, ' and ', id_words_trim{j(idx)}, ' = ', num2str(element)]);
        
        % append pair to our resulting 3-wide cell array
        newrow = { id_words_trim{i(idx)}, id_words_trim{j(idx)}, element };
        c = [c ; newrow];
    end
end
    