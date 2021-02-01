


library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
library work;
use work.ParallelPatterns_pkg.all; 


entity DropEmpty is
  generic (
    INDEX_WIDTH : integer := 32;
    TAG_WIDTH   : integer := 1
  );
  port (
    clk                          : in  std_logic;
    reset                        : in  std_logic;
    
    in_valid                     : in  std_logic;
    in_dvalid                    : in  std_logic;
    in_ready                     : out std_logic;
    
    out_valid                    : out std_logic;
    out_ready                    : in  std_logic
    
  );
end entity;

architecture Behavioral of DropEmpty is  
  signal in_ready_s : std_logic; 
begin

  comb_proc: process(in_valid, in_dvalid, out_ready) is
    begin
      in_ready_s <= out_ready;
      out_valid <= '0';
      if in_valid = '1' then
        if in_dvalid = '1' then
          out_valid <= '1';
        else
          in_ready_s <= '1';
        end if;
      end if;
  end process;
 
  in_ready <= in_ready_s;
 
end Behavioral;
