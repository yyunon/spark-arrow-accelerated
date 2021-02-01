----------------------------------------------------------------------------------
-- Company: 
-- Engineer: 
-- 
-- Create Date: 06/13/2020 10:50:34 AM
-- Design Name: 
-- Module Name: BatchIn_Map - Behavioral
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


library IEEE;
use IEEE.STD_LOGIC_1164.ALL;

library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;
use work.ParallelPatterns_pkg.all;


entity BatchIn_Map is
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
   
end BatchIn_Map;

architecture Behavioral of BatchIn_Map is

  -- Stream to kernel
  signal krnl_out_valid          : std_logic;
  signal krnl_out_ready          : std_logic;
  signal krnl_out_dvalid         : std_logic;
  signal krnl_out_count          : std_logic_vector(1 downto 0);
  signal krnl_out_data           : std_logic_vector(127 downto 0);
  

  -- Stream from kernel
  signal krnl_in_valid           : std_logic;
  signal krnl_in_ready           : std_logic;
  signal krnl_in_dvalid          : std_logic;
  signal krnl_in_count           : std_logic_vector(1 downto 0);
  signal krnl_in_data            : std_logic_vector(127 downto 0);


begin

 map_cntrl : MapStream
    generic map (
      IN_DIMENSIONALITY    => 1,
      IN_COUNT_WIDTH       => 2,
      LENGTH_WIDTH         => 16,
      LENGTH_BUFFER_DEPTH  => 8
    )
    port map (
    clk                          => kcd_clk,
    reset                        => kcd_reset,
      
      -- Input stream.
    in_valid                     => BatchIn_vectors_valid,
    in_ready                     => BatchIn_vectors_ready,
    in_dvalid                    => BatchIn_vectors_dvalid,
    in_count                     => BatchIn_vectors_count,
    in_last(0)                   => BatchIn_vectors_last,
                                 
    -- Stream to kernel 
    krnl_out_valid               => krnl_out_valid,
    krnl_out_ready               => krnl_out_ready,
    krnl_out_dvalid              => krnl_out_dvalid,
    krnl_out_count               => krnl_out_count,
 
    -- Stream from kernel  
    krnl_in_valid                => krnl_in_valid,
    krnl_in_ready                => krnl_in_ready,
    krnl_in_dvalid               => krnl_in_dvalid,
    krnl_in_count                => krnl_in_count,
 
    -- Output stream             
    out_valid                    => BatchOut_vectors_valid,
    out_ready                    => BatchOut_vectors_ready,
    out_dvalid                   => BatchOut_vectors_dvalid,
    out_count                    => BatchOut_vectors_count,
    out_last(0)                  => BatchOut_vectors_last
    );
    
    
    krnl_in_dvalid <= '1';
    
    --Test kernel
    dly: StreamSliceArray
    generic map (
      DATA_WIDTH                 => 130,
      DEPTH                      => 10
    )
    port map (
      clk                       => kcd_clk,
      reset                     => kcd_reset,

      in_valid                  => krnl_out_valid,
      in_ready                  => krnl_out_ready,
      in_data(127 downto 0)     => BatchIn_vectors,
      in_data(129 downto 128)   => krnl_out_count,

      out_valid                 => krnl_in_valid,
      out_ready                 => krnl_in_ready,
      out_data(127 downto 0)    => BatchOut_vectors,
      out_data(129 downto 128)  => krnl_in_count

    );

end Behavioral;
