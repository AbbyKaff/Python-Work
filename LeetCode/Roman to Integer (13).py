class Solution:
    def romanToInt(self, x: str) -> int:
         # largest to smallest: add them up 
         # smaller before larger: subtract smaller
        
        roman = {"I":1, "V":5, "X":10, "L":50,"C":100, "D":500,"M":1000}
        
        output = 0 
        
        for i in range(len(x)):
            if i + 1 < len(x) and roman[x[i]] < roman[x[i+1]]:
                output -= roman[x[i]]
            else:
                output += roman[x[i]]
        return output
        
