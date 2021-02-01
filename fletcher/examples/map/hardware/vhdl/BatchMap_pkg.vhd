----------------------------------------------------------------------------------
-- Company: 
-- Engineer: Ákos Hadnagy
-- 
-- Create Date: 05/29/2020 03:41:48 PM
-- Design Name: 
-- Module Name: SequenceStream - Behavioral
-- Project Name: 
-- Target Devices: 
-- Tool Versions: 
-- Description: 
-- 
-- Dependencies: 
-- 
-- Revision:
-- Revision 0.01 - File Created
-- Additional Comments:
-- 
----------------------------------------------------------------------------------

library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

package BatchMap_pkg is

  component BatchIn_Map is
  port (
    kcd_clk                       : in  std_logic;
    kcd_reset                     : in  std_logic;
    BatchIn_vectors_valid         : in  std_logic;
    BatchIn_vectors_ready         : out std_logic;
    BatchIn_vectors_dvalid        : in  std_logic;
    BatchIn_vectors_last          : in  std_logic;
    BatchIn_vectors               : in  std_logic_vector(127 downto 0);
    BatchIn_vectors_count         : in  std_logic_vector(1 downto 0);
    
    BatchOut_vectors_valid        : out std_logic;
    BatchOut_vectors_ready        : in  std_logic;
    BatchOut_vectors_dvalid       : out std_logic;
    BatchOut_vectors_last         : out std_logic;
    BatchOut_vectors              : out std_logic_vector(127 downto 0);
    BatchOut_vectors_count        : out std_logic_vector(1 downto 0)
    );
   
end component;

end BatchMap_pkg;
