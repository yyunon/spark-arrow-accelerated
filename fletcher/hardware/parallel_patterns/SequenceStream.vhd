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

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;

-- This unit increases the dimensionality of a stream by sequencing it according to the incoming length values.
-- The data lanes should be connected outside this module.

entity SequenceStream is
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
end SequenceStream;

architecture Behavioral of SequenceStream is  
  
  -- Internal sequence counter
  signal remaining              : signed(LENGTH_WIDTH downto 0);
  signal remaining_next         : signed(LENGTH_WIDTH downto 0);
  
  -- Length buffer ourput stream.
  signal b_valid                : std_logic;
  signal b_ready                : std_logic;
  signal b_data                 : std_logic_vector(LENGTH_WIDTH-1 downto 0);

  -- Internal "copies" of the in_ready and out_valid control output signals.
  signal in_ready_s             : std_logic;
  signal out_valid_s            : std_logic;
  signal out_last_s             : std_logic;
  
  -- 

begin

length_buffer: StreamBuffer
    generic map (
      MIN_DEPTH                 => MIN_BUFFER_DEPTH,
      DATA_WIDTH                => LENGTH_WIDTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => in_length_valid,
      in_ready                  => in_length_ready,
      in_data                   => in_length_data,
      out_valid                 => b_valid,
      out_ready                 => b_ready,
      out_data                  => b_data
    );
    
comb_proc: process (in_valid, out_ready, in_count, in_dvalid, b_valid, b_ready, b_data, remaining) is
    variable diff           : signed(LENGTH_WIDTH downto 0);
    variable remaining_var  : signed(LENGTH_WIDTH downto 0);
    variable last           : std_logic;
  begin
    -- We're ready for new data on the input if the output is ready.
      in_ready_s <= out_ready;
      out_last_s <= '0';
      out_valid_s <= in_valid;
      
      remaining_var := remaining;
      
      --If the module is operating in blocking mode, block the
      --input while waiting for a new length value
      
      if BLOCKING and b_ready = '1' then
        in_ready_s <= '0';
      end if;
      
      diff := signed(remaining) - signed('0' & in_count);      
          
      --Last is asserted if we reached the end of the sequence.    
      if diff = 0 and in_dvalid = '1' then
        last := '1';
      else
        last := '0';
      end if;
      
      --If the handshaked data is not valid, the previous count is kept.        
      if in_valid = '1' and out_ready ='1' and in_dvalid = '1' then
        remaining_var := diff;
      end if;
      
      -- If a new length value is being handshaked, the count is adjusted.
      if b_ready = '1' and b_valid = '1' then
        remaining_var := remaining_var + signed(b_data);
      end if;
      
      if in_valid = '1' then
        out_last_s <= last;        
      end if;
      
      remaining_next <= remaining_var;
     
  end process;
    
  reg_proc: process (clk) is
      variable remaining_var        : signed(LENGTH_WIDTH downto 0);
    begin
      if rising_edge(clk) then
          
        
        if b_ready = '1' and b_valid = '1' then
          b_ready <= '0';
        end if;
          
        -- Length buffer ready should be zero by default, only querying a value when it's necessary.
        b_ready <= '0';
          
        -- When the current sequency is fulfilled, a new length value is requested.
        if remaining_next <= 0 then 
          b_ready <= '1';
        end if;
        
        remaining <= remaining_next;
          
        if reset = '1' then
          remaining <= to_signed(0, LENGTH_WIDTH+1);
        end if;
      end if;
  end process;

  in_ready <= in_ready_s;
  out_valid <= out_valid_s;
  out_last <= out_last_s;
    
end Behavioral;
