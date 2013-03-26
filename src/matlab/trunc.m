function outstr = trunc( instr)
%TRUNC Summary of this function goes here
%   Detailed explanation goes here

max=60;
s = size(instr,2);
if (s > max) 
    s = max;
end
outstr = instr(1:s);

end

