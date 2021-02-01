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

package ParallelPatterns_pkg is

  component SequenceStream is
    generic (

    -- Minimum depth of the length buffer
    MIN_BUFFER_DEPTH            : natural := 1;
    
    -- Width of the lenght input and internal counter.
    LENGTH_WIDTH                : natural;
    
    -- Width of the input count field.
    IN_COUNT_WIDTH              : natural;

    -- No transaction is accepted on the data stream when there's no handshaked sequence length in the buffer.
    -- In case of a non-blocking setup, incoming trasactions are accepted and the counter is started in advance.
    -- In this case, the source has to make sure that there are less incoming values than the next arriving length value.
    BLOCKING                    : boolean := false
  );
  port (

    -- Rising-edge sensitive clock.
    clk                         : in  std_logic;

    -- Active-high synchronous reset.
    reset                       : in  std_logic;

    -- Input data stream.
    in_valid                    : in  std_logic;
    in_ready                    : out std_logic;
    in_count                    : in  std_logic_vector(IN_COUNT_WIDTH-1 downto 0) := std_logic_vector(to_unsigned(1, IN_COUNT_WIDTH));
    in_dvalid                   : in  std_logic := '1';
    
    -- Input size stream.
    in_length_valid             : in  std_logic;
    in_length_ready             : out std_logic;
    in_length_data              : in  std_logic_vector(LENGTH_WIDTH-1 downto 0) := std_logic_vector(to_unsigned(1, LENGTH_WIDTH));

    -- Output stream.
    out_valid                   : out std_logic;
    out_ready                   : in  std_logic;
    out_last                    : out std_logic
  );
  end component;
  
  component StreamSliceArray is
  generic (

    -- Width of the stream data vector.
    DATA_WIDTH                  : natural;
    
    -- Numeber of chained slices
    DEPTH                       : natural

  );
  port (

    -- Rising-edge sensitive clock.
    clk                         : in  std_logic;

    -- Active-high synchronous reset.
    reset                       : in  std_logic;

    -- Input stream.
    in_valid                    : in  std_logic;
    in_ready                    : out std_logic;
    in_data                     : in  std_logic_vector(DATA_WIDTH-1 downto 0);

    -- Output stream.
    out_valid                   : out std_logic;
    out_ready                   : in  std_logic;
    out_data                    : out std_logic_vector(DATA_WIDTH-1 downto 0)

  );
  end component;
  
  component FilterStream is
  generic (
    
    -- Width of the stream data vector.
    LANE_COUNT                  : natural;
    
    -- Width of the transaction index.
    INDEX_WIDTH                 : natural;
    
    -- Width of the stream data vector.
    DIMENSIONALITY              : natural := 1;
    
    -- Minimum depth of the transaction buffer
    MIN_BUFFER_DEPTH            : natural := 1

  );
  port (

    -- Rising-edge sensitive clock.
    clk                                 : in  std_logic;

    -- Active-high synchronous reset.
    reset                               : in  std_logic;

    -- Input stream.
    in_valid                            : in  std_logic;
    in_ready                            : out std_logic;
    in_last                             : in  std_logic_vector(DIMENSIONALITY-1 downto 0);
    
    -- Predicate boolean stream.
    pred_in_valid                       : in  std_logic;
    pred_in_ready                       : out std_logic;
    pred_in_data                        : in  std_logic_vector(LANE_COUNT-1 downto 0);
    
    -- Output stream.
    out_valid                           : out std_logic;
    out_ready                           : in  std_logic;
    out_strb                            : out std_logic_vector(LANE_COUNT-1 downto 0);
    out_last                            : out std_logic_vector(LANE_COUNT*DIMENSIONALITY-1 downto 0)
  );
  end component;
  
  component StreamAccumulator is
   generic (
    
    -- Width of the stream data vector.
    DATA_WIDTH                  : natural
    
  );
  port (

    -- Rising-edge sensitive clock.
    clk                         : in  std_logic;

    -- Active-high synchronous reset.
    reset                       : in  std_logic;
    
    -- Init value
    -- Loaded at reset and on 'last'.
    init_value                  : in std_logic_vector(DATA_WIDTH-1 downto 0);

    -- Input stream.
    in_valid                    : in  std_logic;
    in_ready                    : out std_logic;
    in_data                     : in  std_logic_vector(DATA_WIDTH-1 downto 0);
    in_last                     : in  std_logic;
    in_dvalid                   : in  std_logic := '1';

    -- Output stream.
    out_valid                   : out std_logic;
    out_ready                   : in  std_logic;
    out_data                    : out std_logic_vector(DATA_WIDTH-1 downto 0)
    
  );
  end component;
  
  component ReduceStream is
  generic (

    -- Width of the stream data vector.
    DATA_WIDTH                  : natural;
    
    -- Width of the stream data vector.
    IN_DIMENSIONALITY           : natural := 1;
    
    -- Bitwidth of the sequence counter
    LENGTH_WIDTH                : natural := 8
    

  );
  port (

    -- Rising-edge sensitive clock.
    clk                         : in  std_logic;

    -- Active-high synchronous reset.
    reset                       : in  std_logic;
    
    -- Init value for the accumulator.
    acc_init_value              : in std_logic_vector(DATA_WIDTH-1 downto 0);

    -- Input stream.
    in_valid                    : in  std_logic;
    in_ready                    : out std_logic;
    in_last                     : in  std_logic_vector(IN_DIMENSIONALITY-1 downto 0);
    
    -- Accumulator output stream.
    acc_out_valid               : out  std_logic;
    acc_out_ready               : in   std_logic;
    acc_out_data                : out  std_logic_vector(DATA_WIDTH-1 downto 0);
    acc_in_dvalid               : in   std_logic := '1';
    
    -- Accumulator input stream.
    acc_in_valid                : in  std_logic;
    acc_in_ready                : out std_logic;
    acc_in_data                 : in  std_logic_vector(DATA_WIDTH-1 downto 0);

    -- Output stream.
    out_valid                   : out std_logic;
    out_ready                   : in  std_logic;
    out_data                    : out std_logic_vector(DATA_WIDTH-1 downto 0);
    out_last                    : out std_logic_vector(IN_DIMENSIONALITY-2 downto 0)
  );
  end component;
  
  component MapStream is
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
  end component;
  
  component DropEmpty is
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
  end component;

end ParallelPatterns_pkg;
