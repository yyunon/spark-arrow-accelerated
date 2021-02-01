----------------------------------------------------------------------------------
-- Company: 
-- Engineer: 
-- 
-- Create Date: 06/13/2020 03:04:30 PM
-- Design Name: 
-- Module Name: MapStream - Behavioral
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

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;
use work.ParallelPatterns_pkg.all;

entity MapStream is
  generic (
    
    -- Width of the stream data vector.
    IN_DIMENSIONALITY           : natural := 1;
    
    -- In count width.
    IN_COUNT_WIDTH              : natural := 1;
    
    -- Bitwidth of the sequence counter.
    LENGTH_WIDTH                : natural := 8;
    
    -- Sequence length buffer depth.
    LENGTH_BUFFER_DEPTH         : natural := 8

  );
  port (

    -- Rising-edge sensitive clock.
    clk                          : in  std_logic;

    -- Active-high synchronous reset.
    reset                        : in  std_logic;
    

    -- Input stream.
    in_valid                     : in  std_logic;
    in_ready                     : out std_logic;
    in_dvalid                    : in  std_logic;
    in_count                     : in  std_logic_vector(IN_COUNT_WIDTH-1 downto 0) := std_logic_vector(to_unsigned(1, IN_COUNT_WIDTH));
    in_last                      : in  std_logic_vector(IN_DIMENSIONALITY-1 downto 0);
    
    -- Stream to kernel
    krnl_out_valid               : out std_logic;
    krnl_out_ready               : in  std_logic;
    krnl_out_dvalid              : out std_logic;
    krnl_out_count               : out std_logic_vector(IN_COUNT_WIDTH-1 downto 0) := std_logic_vector(to_unsigned(1, IN_COUNT_WIDTH));
    
    -- Stream from kernel
    krnl_in_valid                : in  std_logic;
    krnl_in_ready                : out std_logic;
    krnl_in_dvalid               : in  std_logic;
    krnl_in_count                : in  std_logic_vector(IN_COUNT_WIDTH-1 downto 0) := std_logic_vector(to_unsigned(1, IN_COUNT_WIDTH));

    -- Output stream.
    out_valid                    : out std_logic;
    out_ready                    : in  std_logic;
    out_dvalid                   : out std_logic;
    out_count                    : out std_logic_vector(IN_COUNT_WIDTH-1 downto 0) := std_logic_vector(to_unsigned(1, IN_COUNT_WIDTH));
    out_last                     : out std_logic_vector(IN_DIMENSIONALITY-1 downto 0)
  );
end MapStream;

architecture Behavioral of MapStream is
  
  signal count_valid                : std_logic;
  signal count_ready                : std_logic;
  signal count                      : std_logic_vector(LENGTH_WIDTH-1 downto 0);
  signal count_last                 : std_logic;

  signal counter_valid              : std_logic;
  signal counter_ready              : std_logic;
  
  signal seq_valid                  : std_logic;
  signal seq_ready                  : std_logic;
  signal seq_last                   : std_logic;
  
begin

  in_sync: StreamSync
    generic map (
      NUM_INPUTS                => 1,
      NUM_OUTPUTS               => 2
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      in_valid(0)               => in_valid,
      in_ready(0)               => in_ready,


      out_valid(0)              => counter_valid,
      out_valid(1)              => krnl_out_valid,
      out_ready(0)              => counter_ready,
      out_ready(1)              => krnl_out_ready
      );

  element_counter: StreamElementCounter
    generic map (
      IN_COUNT_MAX              => IN_COUNT_WIDTH**2-1,
      IN_COUNT_WIDTH            => IN_COUNT_WIDTH,
      OUT_COUNT_MAX             => LENGTH_WIDTH**2-1,
      OUT_COUNT_WIDTH           => LENGTH_WIDTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => counter_valid,
      in_ready                  => counter_ready,
      in_dvalid                 => '1',
      in_count                  => in_count,
      in_last                   => in_last(0),
      out_valid                 => count_valid,
      out_ready                 => count_ready,
      out_last                  => count_last,
      out_count                 => count
    );
    
    
 sequencer: SequenceStream
    generic map (
      MIN_BUFFER_DEPTH           => 10,
      IN_COUNT_WIDTH             => IN_COUNT_WIDTH,
      LENGTH_WIDTH               => LENGTH_WIDTH,
      BLOCKING                   => false
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => krnl_in_valid,
      in_ready                  => krnl_in_ready,
      in_dvalid                 => '1',
      in_count                  => krnl_in_count,
      in_length_valid           => count_valid,
      in_length_ready           => count_ready,
      in_length_data            => count,
      out_valid                 => seq_valid,
      out_ready                 => seq_ready,
      out_last                  => seq_last
    );
    
    out_sync: StreamSync
    generic map (
      NUM_INPUTS                => 2,
      NUM_OUTPUTS               => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      in_valid(0)               => seq_valid,
      in_valid(1)               => krnl_in_valid,
      in_ready(0)               => seq_ready,
      in_ready(1)               => krnl_in_ready,


      out_valid(0)              => out_valid,
      out_ready(0)              => out_ready

    );
    
  --Connect count vectors
  krnl_out_count <= in_count;
  out_count <= krnl_in_count;
    
  -- Connect the output 'last' vector
  out_last(0) <= seq_last;
  out_last(IN_DIMENSIONALITY-1 downto 1) <= in_last(IN_DIMENSIONALITY-1 downto 1);
  
  --Connect data valids
  krnl_out_dvalid <= in_dvalid;
  out_dvalid <= krnl_in_dvalid;

end Behavioral;
